from datetime import date, datetime, timedelta, timezone
import re
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.models import (
    CurrentPrice,
    FinanceObservation,
    MsrpObservation,
    MsrpSource,
    PriceHistory,
    ReviewCase,
    ReviewDecision,
    ScrapeBatch,
)
from app.infra import msrp_repository as msrp_repo
from app.infra import review_repository as review_repo
from app.services.fx_service import convert_amount_to_eur
from app.services.msrp_mapping_service import (
    RESOLVER_KIND_LINK,
    RESOLVER_KIND_OVERRIDE,
    apply_canonical_mapping,
)
from app.services.payload_serializers import (
    current_price_payload,
    finance_observation_payload,
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
DEFAULT_PRICE_ALERT_THRESHOLD_PCT = 3.0
FINANCE_CONTEXT_FIELDS = (
    "price_semantics",
    "monthly_payment",
    "down_payment",
    "down_payment_pct",
    "term_months",
    "apr",
    "effective_apr",
    "balloon_payment",
    "finance_type",
    "total_credit_cost",
    "total_amount_payable",
    "annual_mileage_limit",
    "offer_valid_until",
    "subsidy_amount",
    "net_price_after_subsidy",
    "finance_currency",
)
FINANCE_AMOUNT_FIELDS = {
    "monthly_payment",
    "down_payment",
    "balloon_payment",
    "total_credit_cost",
    "total_amount_payable",
    "subsidy_amount",
    "net_price_after_subsidy",
}
AUTO_REVIEW_SCHEMA_VERSION = "msrp_auto_review_score_v1"
AUTO_REVIEW_DEFAULT_WEIGHTS = {
    "match_confidence": 0.30,
    "identity_alignment": 0.22,
    "price_integrity": 0.18,
    "source_traceability": 0.14,
    "semantic_alignment": 0.10,
    "finance_completeness": 0.06,
}
AUTO_REVIEW_FORCE_REVIEW_SCORE = 60.0
AUTO_REVIEW_OFFICIAL_SOURCE_TYPES = {
    "official_api",
    "official_configurator",
    "official_price_list",
    "official_price_list_pdf",
    "official_website",
    "manufacturer_site",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _business_powertrain(value: str | None) -> str:
    return str(value or "").strip()


def _optional_text(value: object | None) -> str | None:
    text = str(value or "").strip()
    return text or None


def _optional_float(value: object | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)


def _optional_int(value: object | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(float(text))


def _optional_date(value: object | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid finance offer_valid_until: {value}",
        ) from exc


def _json_safe_finance_context(context: dict[str, object]) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for key, value in context.items():
        if isinstance(value, (date, datetime)):
            normalized[key] = value.isoformat()
        else:
            normalized[key] = value
    return normalized


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(value, maximum))


def _normal_text(value: object | None) -> str:
    return str(value or "").strip().lower()


def _text_tokens(value: object | None) -> set[str]:
    return {
        token
        for token in re.split(r"[^a-z0-9]+", _normal_text(value))
        if token
    }


def _token_similarity(left: object | None, right: object | None) -> float:
    left_text = _normal_text(left)
    right_text = _normal_text(right)
    if not left_text and not right_text:
        return 0.5
    if not left_text or not right_text:
        return 0.0
    if left_text == right_text:
        return 1.0
    if left_text in right_text or right_text in left_text:
        return 0.9
    left_tokens = _text_tokens(left_text)
    right_tokens = _text_tokens(right_text)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    return overlap / max(len(left_tokens), len(right_tokens))


def _valid_http_url(value: object | None) -> bool:
    parsed = urlparse(str(value or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _dict_or_empty(value: object | None) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _auto_review_config_from_payload(
    item: dict[str, object],
    key: str,
) -> dict[str, Any]:
    source_context = _dict_or_empty(item.get("source_context_json"))
    pricing_context = _dict_or_empty(source_context.get("pricingContext"))
    for candidate in (
        item.get(key),
        item.get(f"{key}_json"),
        source_context.get(key),
        pricing_context.get(key),
    ):
        if isinstance(candidate, dict):
            return candidate
    return {}


def _auto_review_weights(item: dict[str, object]) -> dict[str, float]:
    configured = _auto_review_config_from_payload(
        item,
        "autoReviewWeights",
    ) or _auto_review_config_from_payload(item, "auto_review_weights")
    raw_weights = dict(AUTO_REVIEW_DEFAULT_WEIGHTS)
    for key in AUTO_REVIEW_DEFAULT_WEIGHTS:
        value = configured.get(key)
        if value is None:
            continue
        try:
            weight = float(value)
        except (TypeError, ValueError):
            continue
        raw_weights[key] = max(0.0, weight)

    total = sum(raw_weights.values())
    if total <= 0:
        raw_weights = dict(AUTO_REVIEW_DEFAULT_WEIGHTS)
        total = sum(raw_weights.values())
    return {
        key: round(value / total, 4)
        for key, value in raw_weights.items()
    }


def _auto_review_gate(item: dict[str, object]) -> dict[str, float]:
    configured = _auto_review_config_from_payload(
        item,
        "autoReviewGate",
    ) or _auto_review_config_from_payload(item, "auto_review_gate")
    force_review_score = configured.get("forceReviewBelowScore")
    min_confidence = configured.get("forceReviewBelowConfidence")
    try:
        score_threshold = float(force_review_score)
    except (TypeError, ValueError):
        score_threshold = AUTO_REVIEW_FORCE_REVIEW_SCORE
    try:
        confidence_threshold = float(min_confidence)
    except (TypeError, ValueError):
        confidence_threshold = 0.0
    return {
        "forceReviewBelowScore": round(score_threshold, 2),
        "forceReviewBelowConfidence": round(confidence_threshold, 4),
    }


def _component(
    key: str,
    label: str,
    score: float,
    weight: float,
    evidence: dict[str, object],
) -> dict[str, object]:
    normalized = round(_clamp(score), 4)
    weighted = round(normalized * weight * 100, 2)
    return {
        "key": key,
        "label": label,
        "score": normalized,
        "weight": weight,
        "weightedScore": weighted,
        "evidence": evidence,
    }


def _identity_alignment_score(item: dict[str, object]) -> tuple[float, dict[str, object]]:
    model_score = _token_similarity(
        item.get("jato_model"),
        item.get("official_model"),
    )
    trim_score = _token_similarity(
        item.get("jato_trim"),
        item.get("official_trim"),
    )
    jato_powertrain = _normal_text(item.get("jato_powertrain"))
    official_powertrain = _normal_text(item.get("official_powertrain"))
    if not jato_powertrain and not official_powertrain:
        powertrain_score = 0.75
    elif jato_powertrain and official_powertrain:
        powertrain_score = _token_similarity(
            jato_powertrain,
            official_powertrain,
        )
    else:
        powertrain_score = 0.45
    score = (model_score * 0.58) + (trim_score * 0.27) + (powertrain_score * 0.15)
    return score, {
        "modelScore": round(model_score, 4),
        "trimScore": round(trim_score, 4),
        "powertrainScore": round(powertrain_score, 4),
        "jatoModel": item.get("jato_model"),
        "officialModel": item.get("official_model"),
        "jatoTrim": item.get("jato_trim"),
        "officialTrim": item.get("official_trim"),
    }


def _price_integrity_score(
    source_msrp_value: float,
    source_currency: str,
) -> tuple[float, dict[str, object], list[str]]:
    blockers: list[str] = []
    price_ok = source_msrp_value > 0
    currency_ok = len(source_currency.strip()) == 3
    if not price_ok:
        blockers.append("invalid_price")
    if not currency_ok:
        blockers.append("missing_currency")
    return (
        (0.72 if price_ok else 0.0) + (0.28 if currency_ok else 0.0),
        {
            "sourceMsrpValue": source_msrp_value,
            "sourceCurrency": source_currency,
            "pricePositive": price_ok,
            "currencyPresent": currency_ok,
        },
        blockers,
    )


def _source_traceability_score(
    item: dict[str, object],
    source: MsrpSource,
) -> tuple[float, dict[str, object], list[str]]:
    source_type = str(getattr(source, "source_type", "") or "").strip()
    source_url = str(item.get("source_url") or "").strip()
    registry_url = str(getattr(source, "source_url", "") or "").strip()
    extractor_name = str(getattr(source, "extractor_name", "") or "").strip()
    extractor_version = str(
        getattr(source, "extractor_version", "") or ""
    ).strip()
    source_url_ok = _valid_http_url(source_url)
    registry_url_ok = _valid_http_url(registry_url)
    official_source = source_type in AUTO_REVIEW_OFFICIAL_SOURCE_TYPES
    extractor_ok = bool(extractor_name and extractor_version)
    snapshot_ok = bool(str(item.get("source_snapshot_path") or "").strip())
    score = (
        (0.30 if source_url_ok else 0.0)
        + (0.20 if registry_url_ok else 0.0)
        + (0.25 if official_source else 0.0)
        + (0.15 if extractor_ok else 0.0)
        + (0.10 if snapshot_ok else 0.0)
    )
    blockers = [] if source_url_ok else ["missing_source_url"]
    return score, {
        "sourceUrlPresent": source_url_ok,
        "registryUrlPresent": registry_url_ok,
        "sourceType": source_type or None,
        "officialSourceType": official_source,
        "extractor": extractor_name or None,
        "extractorVersion": extractor_version or None,
        "snapshotPresent": snapshot_ok,
    }, blockers


def _semantic_alignment_score(
    item: dict[str, object],
    source_price_semantics: str | None,
) -> tuple[float, dict[str, object]]:
    price_semantics = _payload_price_semantics(item, source_price_semantics)
    label = _normal_text(item.get("price_label"))
    context = _finance_context_from_payload(item, source_price_semantics)
    finance_semantics = price_semantics != "base_msrp"
    finance_signal = any(
        context.get(key) is not None
        for key in FINANCE_CONTEXT_FIELDS
        if key != "price_semantics"
    )
    if finance_semantics:
        score = 0.75 if finance_signal else 0.35
        if "monthly" in price_semantics or "lease" in price_semantics:
            score += 0.15
        if "lease" in label or "monthly" in label or "mån" in label:
            score += 0.10
    else:
        finance_words = ("lease", "monthly", "finance", "subscription", "mån")
        score = 0.55 if any(word in label for word in finance_words) else 1.0
    return _clamp(score), {
        "priceSemantics": price_semantics,
        "sourcePriceSemantics": source_price_semantics,
        "hasFinanceSignal": finance_signal,
        "priceLabel": item.get("price_label"),
    }


def _finance_completeness_score(
    item: dict[str, object],
    source_price_semantics: str | None,
) -> tuple[float, dict[str, object]]:
    price_semantics = _payload_price_semantics(item, source_price_semantics)
    if price_semantics == "base_msrp":
        return 1.0, {"priceSemantics": price_semantics, "notApplicable": True}
    context = _finance_context_from_payload(item, source_price_semantics)
    has_monthly = context.get("monthly_payment") is not None
    has_term = context.get("term_months") is not None
    has_type = bool(_optional_text(context.get("finance_type")))
    has_subsidy_or_net = (
        context.get("subsidy_amount") is not None
        or context.get("net_price_after_subsidy") is not None
    )
    score = (
        (0.55 if has_monthly else 0.0)
        + (0.18 if has_term else 0.0)
        + (0.17 if has_type else 0.0)
        + (0.10 if has_subsidy_or_net else 0.0)
    )
    return score, {
        "priceSemantics": price_semantics,
        "hasMonthlyPayment": has_monthly,
        "hasTermMonths": has_term,
        "hasFinanceType": has_type,
        "hasSubsidyOrNetPrice": has_subsidy_or_net,
    }


def _model_assistance_recommendation(
    score: float,
    blockers: list[str],
    components: list[dict[str, object]],
) -> dict[str, object]:
    component_scores = {
        str(item["key"]): float(item["score"])
        for item in components
    }
    semantic_low = component_scores.get("semantic_alignment", 1.0) < 0.7
    identity_low = component_scores.get("identity_alignment", 1.0) < 0.7
    traceability_low = component_scores.get("source_traceability", 1.0) < 0.6
    if blockers:
        preferred = "rule_based_source_repair"
        llm_fit = "low"
        rationale = (
            "Hard blockers are fetch/source-quality issues, "
            "not language understanding issues."
        )
    elif semantic_low or identity_low:
        preferred = "rule_based_then_llm"
        llm_fit = "medium"
        rationale = (
            "LLM can help classify page semantics or trim naming after "
            "deterministic checks flag uncertainty."
        )
    elif traceability_low:
        preferred = "rule_based_source_repair"
        llm_fit = "low"
        rationale = "Traceability gaps require source metadata or snapshot repair."
    elif score < 75:
        preferred = "rule_based_recheck"
        llm_fit = "low"
        rationale = "The low score is better handled by deterministic profile/source fixes first."
    else:
        preferred = "deterministic_rules"
        llm_fit = "low"
        rationale = "Weighted deterministic checks are sufficient for this observation."
    return {
        "preferred": preferred,
        "llmFit": llm_fit,
        "neuralNetworkFit": "not_recommended_until_labeled_corpus",
        "rationale": rationale,
    }


def _build_msrp_auto_review_score(
    item: dict[str, object],
    source: MsrpSource,
    *,
    source_msrp_value: float,
    source_currency: str,
    match_confidence: float,
) -> dict[str, object]:
    weights = _auto_review_weights(item)
    source_price_semantics = getattr(source, "price_semantics", None)
    identity_score, identity_evidence = _identity_alignment_score(item)
    price_score, price_evidence, price_blockers = _price_integrity_score(
        source_msrp_value,
        source_currency,
    )
    trace_score, trace_evidence, trace_blockers = _source_traceability_score(
        item,
        source,
    )
    semantic_score, semantic_evidence = _semantic_alignment_score(
        item,
        source_price_semantics,
    )
    finance_score, finance_evidence = _finance_completeness_score(
        item,
        source_price_semantics,
    )
    components = [
        _component(
            "match_confidence",
            "Extractor match confidence",
            match_confidence,
            weights["match_confidence"],
            {"matchConfidence": round(match_confidence, 4)},
        ),
        _component(
            "identity_alignment",
            "Official/JATO model, trim and powertrain alignment",
            identity_score,
            weights["identity_alignment"],
            identity_evidence,
        ),
        _component(
            "price_integrity",
            "Price and currency integrity",
            price_score,
            weights["price_integrity"],
            price_evidence,
        ),
        _component(
            "source_traceability",
            "Official source and snapshot traceability",
            trace_score,
            weights["source_traceability"],
            trace_evidence,
        ),
        _component(
            "semantic_alignment",
            "MSRP/finance semantic alignment",
            semantic_score,
            weights["semantic_alignment"],
            semantic_evidence,
        ),
        _component(
            "finance_completeness",
            "Lease/finance context completeness",
            finance_score,
            weights["finance_completeness"],
            finance_evidence,
        ),
    ]
    score = round(
        sum(float(component["weightedScore"]) for component in components),
        2,
    )
    hard_blockers = price_blockers + trace_blockers
    gate = _auto_review_gate(item)
    model_assistance = _model_assistance_recommendation(
        score,
        hard_blockers,
        components,
    )
    return {
        "schemaVersion": AUTO_REVIEW_SCHEMA_VERSION,
        "method": "deterministic_weighted_rules",
        "score": score,
        "scoreBand": (
            "high" if score >= 80 else "medium" if score >= 60 else "low"
        ),
        "weights": weights,
        "components": components,
        "hardBlockers": hard_blockers,
        "gate": gate,
        "modelAssistance": model_assistance,
    }


def _auto_review_adjusted_match_status(
    match_status: str,
    auto_review: dict[str, object],
    match_confidence: float,
) -> str:
    if match_status in {"human_approved", "override_applied"}:
        return match_status
    gate = _dict_or_empty(auto_review.get("gate"))
    score = float(auto_review.get("score") or 0.0)
    hard_blockers = auto_review.get("hardBlockers")
    has_blockers = isinstance(hard_blockers, list) and bool(hard_blockers)
    force_review_score = float(
        gate.get("forceReviewBelowScore") or AUTO_REVIEW_FORCE_REVIEW_SCORE
    )
    force_review_confidence = float(gate.get("forceReviewBelowConfidence") or 0.0)
    if has_blockers or score < force_review_score or match_confidence < force_review_confidence:
        return REVIEW_REQUIRED_STATUS
    return match_status


def _merge_auto_review_match_reason(
    match_reason: object | None,
    auto_review: dict[str, object],
    *,
    input_match_status: str,
    final_match_status: str,
) -> dict[str, object]:
    reason: dict[str, object] = (
        dict(match_reason) if isinstance(match_reason, dict) else {}
    )
    review_payload = dict(auto_review)
    review_payload["inputMatchStatus"] = input_match_status
    review_payload["finalMatchStatus"] = final_match_status
    review_payload["statusAdjusted"] = input_match_status != final_match_status
    reason["autoReview"] = review_payload
    return reason


def _finance_context_from_payload(
    item: dict[str, object],
    source_price_semantics: str | None = None,
) -> dict[str, object]:
    context: dict[str, object] = {}
    source_context = item.get("source_context_json")
    if isinstance(source_context, dict):
        pricing_context = source_context.get("pricingContext")
        if isinstance(pricing_context, dict):
            for key in FINANCE_CONTEXT_FIELDS:
                if pricing_context.get(key) is not None:
                    context[key] = pricing_context[key]

    for key in FINANCE_CONTEXT_FIELDS:
        if item.get(key) is not None:
            context[key] = item[key]

    source_semantics = _optional_text(source_price_semantics)
    context.setdefault("price_semantics", source_semantics or "base_msrp")

    has_finance_values = any(
        context.get(key) is not None
        for key in FINANCE_CONTEXT_FIELDS
        if key != "price_semantics"
    )
    if not has_finance_values and context.get("price_semantics") == "base_msrp":
        return {}
    return _json_safe_finance_context(context)


def _payload_price_semantics(
    item: dict[str, object],
    source_price_semantics: str | None = None,
) -> str:
    explicit_semantics = _optional_text(item.get("price_semantics"))
    if explicit_semantics:
        return explicit_semantics
    return str(source_price_semantics or "base_msrp").strip() or "base_msrp"


def _is_current_price_semantics(price_semantics: str) -> bool:
    return price_semantics == "base_msrp"


def _amount_to_eur(
    value: object | None,
    currency: str,
    observed_at_utc: datetime,
    fallback_rate_to_eur: object,
) -> float | None:
    amount = _optional_float(value)
    if amount is None:
        return None
    normalized_currency = currency.strip().upper()
    if normalized_currency == "EUR":
        return round(amount, 2)
    if normalized_currency:
        converted, _ = convert_amount_to_eur(
            amount,
            normalized_currency,
            observed_at_utc,
        )
        return round(float(converted), 2)
    return round(amount * float(fallback_rate_to_eur), 2)


def _finance_observation_from_payload(
    observation: MsrpObservation,
    item: dict[str, object],
    source_price_semantics: str | None = None,
) -> FinanceObservation | None:
    context = _finance_context_from_payload(item, source_price_semantics)
    if not context:
        return None

    currency = str(
        context.get("finance_currency")
        or context.get("currency")
        or observation.source_currency
    ).strip().upper()
    observed_at_utc = observation.observed_at_utc
    return FinanceObservation(
        observation_id=observation.observation_id,
        scrape_batch_id=observation.scrape_batch_id,
        country=observation.country,
        brand=observation.brand,
        jato_model=observation.jato_model,
        jato_trim=observation.jato_trim,
        jato_powertrain=observation.jato_powertrain,
        official_model=observation.official_model,
        official_trim=observation.official_trim,
        official_edition=observation.official_edition,
        official_powertrain=observation.official_powertrain,
        price_semantics=str(
            context.get("price_semantics") or "base_msrp"
        ).strip(),
        finance_type=_optional_text(context.get("finance_type")),
        monthly_payment=_optional_float(context.get("monthly_payment")),
        monthly_payment_eur=_amount_to_eur(
            context.get("monthly_payment"),
            currency,
            observed_at_utc,
            observation.fx_rate_to_eur,
        ),
        down_payment=_optional_float(context.get("down_payment")),
        down_payment_eur=_amount_to_eur(
            context.get("down_payment"),
            currency,
            observed_at_utc,
            observation.fx_rate_to_eur,
        ),
        down_payment_pct=_optional_float(context.get("down_payment_pct")),
        term_months=_optional_int(context.get("term_months")),
        apr=_optional_float(context.get("apr")),
        effective_apr=_optional_float(context.get("effective_apr")),
        balloon_payment=_optional_float(context.get("balloon_payment")),
        balloon_payment_eur=_amount_to_eur(
            context.get("balloon_payment"),
            currency,
            observed_at_utc,
            observation.fx_rate_to_eur,
        ),
        total_credit_cost=_optional_float(context.get("total_credit_cost")),
        total_credit_cost_eur=_amount_to_eur(
            context.get("total_credit_cost"),
            currency,
            observed_at_utc,
            observation.fx_rate_to_eur,
        ),
        total_amount_payable=_optional_float(
            context.get("total_amount_payable")
        ),
        total_amount_payable_eur=_amount_to_eur(
            context.get("total_amount_payable"),
            currency,
            observed_at_utc,
            observation.fx_rate_to_eur,
        ),
        annual_mileage_limit=_optional_int(
            context.get("annual_mileage_limit")
        ),
        offer_valid_until=_optional_date(context.get("offer_valid_until")),
        subsidy_amount=_optional_float(context.get("subsidy_amount")),
        subsidy_amount_eur=_amount_to_eur(
            context.get("subsidy_amount"),
            currency,
            observed_at_utc,
            observation.fx_rate_to_eur,
        ),
        net_price_after_subsidy=_optional_float(
            context.get("net_price_after_subsidy")
        ),
        net_price_after_subsidy_eur=_amount_to_eur(
            context.get("net_price_after_subsidy"),
            currency,
            observed_at_utc,
            observation.fx_rate_to_eur,
        ),
        currency=currency,
        source_url=observation.source_url,
        observed_at_utc=observed_at_utc,
        finance_context_json=context,
    )


def _commit_or_conflict(
    session: Session,
    detail: str,
    *,
    commit: bool = True,
) -> None:
    try:
        if commit:
            session.commit()
        else:
            session.flush()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=detail) from exc


def materialize_current_price_from_observation(
    session: Session,
    observation: MsrpObservation,
    *,
    price_history_enabled: bool | None = None,
) -> CurrentPrice | None:
    apply_canonical_mapping(session, observation)
    if observation.match_status not in ELIGIBLE_CURRENT_PRICE_STATUSES:
        return None

    current_price = msrp_repo.get_current_price_by_key(
        session,
        observation.country,
        observation.brand,
        observation.jato_model,
        observation.jato_trim,
        observation.jato_powertrain,
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
        current_price.jato_powertrain = _business_powertrain(
            observation.jato_powertrain
        )
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
            jato_powertrain=_business_powertrain(
                observation.jato_powertrain
            ),
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
            observation.jato_powertrain,
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
            observation.jato_powertrain,
        )
    if open_period is not None:
        if observation.observed_at_utc == open_period.valid_from_utc:
            _replace_open_price_period(open_period, observation)
            return
        if observation.observed_at_utc < open_period.valid_from_utc:
            return
        open_period.valid_to_utc = observation.observed_at_utc
        open_period.ended_by_observation_id = observation.observation_id

    new_period = PriceHistory(
        country=observation.country,
        brand=observation.brand,
        jato_model=observation.jato_model,
        jato_trim=observation.jato_trim,
        jato_powertrain=_business_powertrain(observation.jato_powertrain),
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


def _replace_open_price_period(
    open_period: PriceHistory,
    observation: MsrpObservation,
) -> None:
    open_period.msrp_value = observation.msrp_value
    open_period.currency = observation.currency
    open_period.source_msrp_value = observation.source_msrp_value
    open_period.source_currency = observation.source_currency
    open_period.valid_from_utc = observation.observed_at_utc
    open_period.valid_to_utc = None
    open_period.started_by_observation_id = observation.observation_id
    open_period.ended_by_observation_id = None
    open_period.last_confirmed_at_utc = observation.observed_at_utc
    open_period.last_confirmed_by_observation_id = (
        observation.observation_id
    )


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
    *,
    commit: bool = True,
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
    sources: list[MsrpSource] = []
    finance_observations: list[FinanceObservation] = []
    finance_observations_skipped = 0
    price_history_enabled = msrp_repo.has_price_history_table(session)
    finance_observations_enabled = (
        msrp_repo.has_finance_observations_table(session)
    )

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
        sources.append(source)
        observed_at_utc = item.get("observed_at_utc") or _utc_now()
        source_msrp_value = float(item["msrp_value"])
        source_currency = str(item.get("currency") or "").strip().upper()
        match_confidence = float(item.get("match_confidence") or 0.0)
        input_match_status = str(
            item.get("match_status") or REVIEW_REQUIRED_STATUS
        ).strip()
        auto_review = _build_msrp_auto_review_score(
            item,
            source,
            source_msrp_value=source_msrp_value,
            source_currency=source_currency,
            match_confidence=match_confidence,
        )
        match_status = _auto_review_adjusted_match_status(
            input_match_status,
            auto_review,
            match_confidence,
        )
        match_reason_json = _merge_auto_review_match_reason(
            item.get("match_reason_json"),
            auto_review,
            input_match_status=input_match_status,
            final_match_status=match_status,
        )
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
            match_confidence=match_confidence,
            match_status=match_status,
            match_reason_json=match_reason_json,
            source_context_json=item.get("source_context_json"),
        )
        observations.append(observation)

    msrp_repo.add_observations(session, observations)
    session.flush()

    for observation, payload, source in zip(
        observations,
        observations_payload,
        sources,
        strict=False,
    ):
        finance_observation = _finance_observation_from_payload(
            observation,
            payload,
            source.price_semantics,
        )
        if finance_observation is None:
            continue
        if finance_observations_enabled:
            finance_observations.append(finance_observation)
        else:
            finance_observations_skipped += 1
    if finance_observations:
        msrp_repo.add_finance_observations(session, finance_observations)
        session.flush()

    success_count = 0
    review_required_count = 0
    override_applied_count = 0
    link_applied_count = 0
    non_msrp_price_observation_count = 0
    for observation, payload, source in zip(
        observations,
        observations_payload,
        sources,
        strict=False,
    ):
        if observation.match_status == REVIEW_REQUIRED_STATUS:
            resolution = apply_canonical_mapping(session, observation)
            if observation.match_status in ELIGIBLE_CURRENT_PRICE_STATUSES:
                if resolution["resolverKind"] == RESOLVER_KIND_OVERRIDE:
                    override_applied_count += 1
                elif resolution["resolverKind"] == RESOLVER_KIND_LINK:
                    link_applied_count += 1
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
            price_semantics = _payload_price_semantics(
                payload,
                source.price_semantics,
            )
            if not _is_current_price_semantics(price_semantics):
                non_msrp_price_observation_count += 1
                success_count += 1
                continue
            current_price = materialize_current_price_from_observation(
                session,
                observation,
                price_history_enabled=price_history_enabled,
            )
            if current_price is not None:
                current_prices.append(current_price)
                # The app session runs with autoflush disabled. Flush each
                # materialized price so later observations in the same batch
                # can see the current price and open history period.
                _commit_or_conflict(
                    session,
                    "Scrape batch ingest hit a conflict",
                    commit=False,
                )
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

    _commit_or_conflict(
        session,
        "Scrape batch ingest hit a conflict",
        commit=commit,
    )
    session.refresh(batch)
    return {
        "scrapeBatch": scrape_batch_payload(batch),
        "observationRows": len(observations),
        "reviewCasesCreated": len(review_cases),
        "overrideAppliedCount": override_applied_count,
        "linkAppliedCount": link_applied_count,
        "currentPricesTouched": len(current_prices),
        "nonMsrpPriceObservationCount": non_msrp_price_observation_count,
        "financeObservationsCreated": len(finance_observations),
        "financeObservationsSkipped": finance_observations_skipped,
        "sampleObservations": [
            observation_payload(item) for item in observations[:10]
        ],
        "sampleFinanceObservations": [
            finance_observation_payload(item)
            for item in finance_observations[:10]
        ],
        "sampleReviewCases": [
            review_case_payload(item) for item in review_cases[:10]
        ],
        "sampleCurrentPrices": [
            current_price_payload(item) for item in current_prices[:10]
        ],
    }


def _source_by_effective_observation_id(
    session: Session,
    items: list[CurrentPrice],
) -> dict[object, MsrpSource | None]:
    observations = msrp_repo.list_observations_by_ids(
        session,
        [item.effective_observation_id for item in items],
    )
    observation_by_id = {
        item.observation_id: item for item in observations
    }
    sources = msrp_repo.list_sources_by_ids(
        session,
        [item.source_id for item in observations],
    )
    source_by_id = {item.source_id: item for item in sources}
    return {
        observation_id: source_by_id.get(observation.source_id)
        for observation_id, observation in observation_by_id.items()
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
    source_by_observation_id = _source_by_effective_observation_id(
        session,
        items,
    )
    return {
        "rows": len(items),
        "total": total,
        "limit": limit,
        "offset": offset,
        "priceAlertCount": price_alert_count,
        "items": [
            current_price_payload(
                item,
                source_by_observation_id.get(item.effective_observation_id),
            )
            for item in items
        ],
    }


def _increment_count(target: dict[str, int], value: object | None) -> None:
    key = str(value or "").strip() or "unknown"
    target[key] = target.get(key, 0) + 1


def _empty_finance_observation_summary() -> dict[str, object]:
    return {
        "priceSemanticsCounts": {},
        "financeTypeCounts": {},
        "monthlyPaymentCount": 0,
        "monthlyPaymentEurMin": None,
        "monthlyPaymentEurMax": None,
        "netPriceAfterSubsidyCount": 0,
        "netPriceAfterSubsidyEurMin": None,
        "netPriceAfterSubsidyEurMax": None,
        "subsidyObservationCount": 0,
    }


def _summarize_finance_observations(
    items: list[FinanceObservation],
) -> dict[str, object]:
    semantics_counts: dict[str, int] = {}
    finance_type_counts: dict[str, int] = {}
    monthly_values = [
        float(item.monthly_payment_eur)
        for item in items
        if item.monthly_payment_eur is not None
    ]
    monthly_count = sum(
        1
        for item in items
        if item.monthly_payment is not None
        or item.monthly_payment_eur is not None
    )
    net_values = [
        float(item.net_price_after_subsidy_eur)
        for item in items
        if item.net_price_after_subsidy_eur is not None
    ]
    net_count = sum(
        1
        for item in items
        if item.net_price_after_subsidy is not None
        or item.net_price_after_subsidy_eur is not None
    )
    subsidy_values = [
        float(item.subsidy_amount_eur)
        for item in items
        if item.subsidy_amount_eur is not None
    ]
    subsidy_count = sum(
        1
        for item in items
        if item.subsidy_amount is not None
        or item.subsidy_amount_eur is not None
    )
    for item in items:
        _increment_count(semantics_counts, item.price_semantics)
        _increment_count(finance_type_counts, item.finance_type)
    return {
        "priceSemanticsCounts": semantics_counts,
        "financeTypeCounts": finance_type_counts,
        "monthlyPaymentCount": monthly_count,
        "monthlyPaymentEurMin": min(monthly_values) if monthly_values else None,
        "monthlyPaymentEurMax": max(monthly_values) if monthly_values else None,
        "netPriceAfterSubsidyCount": net_count,
        "netPriceAfterSubsidyEurMin": (
            min(net_values) if net_values else None
        ),
        "netPriceAfterSubsidyEurMax": (
            max(net_values) if net_values else None
        ),
        "subsidyObservationCount": subsidy_count,
    }


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
) -> dict[str, object]:
    if not msrp_repo.has_finance_observations_table(session):
        return {
            "rows": 0,
            "total": 0,
            "limit": limit,
            "offset": offset,
            "summary": _empty_finance_observation_summary(),
            "items": [],
            "warning": "finance_observations_unavailable",
        }
    total = msrp_repo.count_finance_observations(
        session,
        country,
        brand,
        jato_model,
        price_semantics,
        finance_type,
        has_monthly_payment,
        has_subsidy,
        has_net_price_after_subsidy,
    )
    summary = msrp_repo.summarize_finance_observations(
        session,
        country,
        brand,
        jato_model,
        price_semantics,
        finance_type,
        has_monthly_payment,
        has_subsidy,
        has_net_price_after_subsidy,
    )
    items = msrp_repo.list_finance_observations(
        session,
        country,
        brand,
        jato_model,
        price_semantics,
        finance_type,
        has_monthly_payment,
        has_subsidy,
        has_net_price_after_subsidy,
        limit,
        offset,
    )
    return {
        "rows": len(items),
        "total": total,
        "limit": limit,
        "offset": offset,
        "summary": summary,
        "items": [finance_observation_payload(item) for item in items],
    }


def _float_or_none(value: object | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _delta_direction(delta_value: float | None) -> str:
    if delta_value is None:
        return "unknown"
    if delta_value > 0:
        return "increase"
    if delta_value < 0:
        return "decrease"
    return "unchanged"


def _price_event_type(
    delta_base: float | None,
    source_currency_changed: bool,
) -> str:
    if source_currency_changed:
        return "currency_change"
    if delta_base is None:
        return "price_change_unknown_delta"
    if delta_base == 0:
        return "price_confirmed"
    return "price_change"


def _price_alert_severity(
    delta_pct: float | None,
    source_currency_changed: bool,
    threshold_pct: float,
) -> str:
    if source_currency_changed:
        return "warning"
    if delta_pct is None:
        return "info"
    abs_delta_pct = abs(delta_pct)
    if abs_delta_pct >= threshold_pct * 2:
        return "critical"
    if abs_delta_pct >= threshold_pct:
        return "warning"
    return "info"


def _price_alert_recommended_action(
    severity: str,
    direction: str,
    source_currency_changed: bool,
) -> str:
    if source_currency_changed:
        return "review_currency_or_source_semantics"
    if severity in {"critical", "warning"}:
        if direction == "decrease":
            return "review_price_drop_and_queue_sales_effectiveness"
        if direction == "increase":
            return "review_price_increase_and_notify_market_team"
        return "review_price_change_event"
    return "keep_monitoring"


def _summarize_price_alert_events(
    items: list[dict[str, object]],
) -> dict[str, object]:
    direction_counts: dict[str, int] = {}
    severity_counts: dict[str, int] = {}
    threshold_alert_count = 0
    high_priority_count = 0
    for item in items:
        direction = str(item.get("direction") or "unknown")
        severity = str(item.get("severity") or "info")
        direction_counts[direction] = direction_counts.get(direction, 0) + 1
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        if bool(item.get("isThresholdAlert")):
            threshold_alert_count += 1
        if bool(item.get("isHighPriority")):
            high_priority_count += 1
    return {
        "priceChangeEventCount": len(items),
        "thresholdAlertCount": threshold_alert_count,
        "highPriorityAlertCount": high_priority_count,
        "directionCounts": direction_counts,
        "severityCounts": severity_counts,
    }


def _price_alert_payload(
    current_price: CurrentPrice,
    latest_period: PriceHistory | None,
    previous_period: PriceHistory | None,
    source: MsrpSource | None,
    threshold_pct: float = DEFAULT_PRICE_ALERT_THRESHOLD_PCT,
) -> dict[str, object]:
    current_source_value = _float_or_none(
        latest_period.source_msrp_value
        if latest_period is not None
        else current_price.source_msrp_value
    )
    previous_source_value = _float_or_none(
        previous_period.source_msrp_value
        if previous_period is not None
        else None
    )
    current_source_currency = (
        latest_period.source_currency
        if latest_period is not None
        else current_price.source_currency
    )
    previous_source_currency = (
        previous_period.source_currency
        if previous_period is not None
        else None
    )
    source_currency_changed = (
        previous_source_currency is not None
        and current_source_currency != previous_source_currency
    )

    delta_source_value = None
    if (
        current_source_value is not None
        and previous_source_value is not None
        and not source_currency_changed
    ):
        delta_source_value = round(
            current_source_value - previous_source_value,
            2,
        )

    current_eur_value = _float_or_none(
        latest_period.msrp_value
        if latest_period is not None
        else current_price.current_msrp_value
    )
    previous_eur_value = _float_or_none(
        previous_period.msrp_value
        if previous_period is not None
        else None
    )
    delta_eur_value = None
    if current_eur_value is not None and previous_eur_value is not None:
        delta_eur_value = round(current_eur_value - previous_eur_value, 2)

    ratio_base = (
        previous_source_value
        if delta_source_value is not None and previous_source_value
        else previous_eur_value
    )
    delta_base = (
        delta_source_value
        if delta_source_value is not None
        else delta_eur_value
    )
    delta_pct = (
        round((delta_base / ratio_base) * 100, 2)
        if delta_base is not None and ratio_base
        else None
    )

    direction = _delta_direction(delta_base)
    event_type = _price_event_type(delta_base, source_currency_changed)
    severity = _price_alert_severity(
        delta_pct,
        source_currency_changed,
        threshold_pct,
    )
    is_threshold_alert = severity in {"critical", "warning"}
    is_high_priority = severity == "critical"
    changed_at = (
        latest_period.valid_from_utc
        if latest_period is not None
        else current_price.last_price_change_at_utc
    )
    return {
        "country": current_price_payload(current_price)["country"],
        "brand": current_price.brand,
        "jatoModel": current_price.jato_model,
        "jatoTrim": current_price.jato_trim,
        "jatoPowertrain": (
            _business_powertrain(current_price.jato_powertrain) or None
        ),
        "eventType": event_type,
        "direction": direction,
        "severity": severity,
        "thresholdPct": threshold_pct,
        "isThresholdAlert": is_threshold_alert,
        "isHighPriority": is_high_priority,
        "recommendedAction": _price_alert_recommended_action(
            severity,
            direction,
            source_currency_changed,
        ),
        "changedAtUtc": changed_at.isoformat() if changed_at else None,
        "currentSourceMsrpValue": current_source_value,
        "previousSourceMsrpValue": previous_source_value,
        "currentSourceCurrency": current_source_currency,
        "previousSourceCurrency": previous_source_currency,
        "sourceCurrencyChanged": source_currency_changed,
        "deltaSourceMsrpValue": delta_source_value,
        "deltaMsrpValue": delta_eur_value,
        "deltaPct": delta_pct,
        "currentPrice": current_price_payload(current_price, source),
        "latestPrice": (
            price_history_payload(latest_period)
            if latest_period is not None
            else None
        ),
        "previousPrice": (
            price_history_payload(previous_period)
            if previous_period is not None
            else None
        ),
    }


def list_current_price_alerts(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    offset: int,
    threshold_pct: float = DEFAULT_PRICE_ALERT_THRESHOLD_PCT,
) -> dict[str, object]:
    threshold_pct = max(0.0, float(threshold_pct))
    if not msrp_repo.has_price_history_table(session):
        return {
            "rows": 0,
            "total": 0,
            "limit": limit,
            "offset": offset,
            "thresholdPct": threshold_pct,
            "summary": _summarize_price_alert_events([]),
            "items": [],
            "warning": "price_history_unavailable",
        }

    total = msrp_repo.count_current_price_alerts(
        session,
        country,
        brand,
        jato_model,
    )
    items = msrp_repo.list_current_price_alerts(
        session,
        country,
        brand,
        jato_model,
        limit,
        offset,
    )
    source_by_observation_id = _source_by_effective_observation_id(
        session,
        items,
    )

    alert_items: list[dict[str, object]] = []
    for item in items:
        history = msrp_repo.list_price_history(
            session,
            item.country,
            item.brand,
            item.jato_model,
            item.jato_trim,
            item.jato_powertrain,
            2,
        )
        latest_period = history[0] if history else None
        previous_period = history[1] if len(history) > 1 else None
        alert_items.append(
            _price_alert_payload(
                item,
                latest_period,
                previous_period,
                source_by_observation_id.get(item.effective_observation_id),
                threshold_pct,
            )
        )

    return {
        "rows": len(alert_items),
        "total": total,
        "limit": limit,
        "offset": offset,
        "thresholdPct": threshold_pct,
        "summary": _summarize_price_alert_events(alert_items),
        "items": alert_items,
    }


def build_current_price_snapshot(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    threshold_pct: float = DEFAULT_PRICE_ALERT_THRESHOLD_PCT,
) -> dict[str, object]:
    generated_at = _utc_now()
    iso_year, iso_week, _ = generated_at.isocalendar()
    current_prices = list_current_prices(
        session,
        country,
        brand,
        jato_model,
        limit,
        0,
    )
    price_alerts = list_current_price_alerts(
        session,
        country,
        brand,
        jato_model,
        limit,
        0,
        threshold_pct,
    )

    return {
        "schemaVersion": "msrp_current_price_snapshot_v1",
        "generatedAtUtc": generated_at.isoformat(),
        "snapshotWeek": f"{iso_year}-W{iso_week:02d}",
        "filters": {
            "country": country,
            "brand": brand,
            "jatoModel": jato_model,
        },
        "summary": {
            "currentPriceCount": current_prices.get("total", 0),
            "returnedCurrentPriceCount": current_prices.get("rows", 0),
            "priceAlertCount": price_alerts.get("total", 0),
            "returnedPriceAlertCount": price_alerts.get("rows", 0),
            "priceAlertThresholdPct": price_alerts.get(
                "thresholdPct",
                threshold_pct,
            ),
            "priceAlertSummary": price_alerts.get(
                "summary",
                _summarize_price_alert_events(
                    [
                        item for item in list(price_alerts.get("items") or [])
                        if isinstance(item, dict)
                    ]
                ),
            ),
            "limit": limit,
        },
        "currentPrices": current_prices.get("items", []),
        "priceAlerts": price_alerts.get("items", []),
        "warnings": [
            item
            for item in [
                current_prices.get("warning"),
                price_alerts.get("warning"),
            ]
            if item
        ],
    }


def _reconciliation_key(
    observation: MsrpObservation,
) -> tuple[str, str, str, str, str]:
    return (
        observation.country,
        observation.brand,
        observation.jato_model,
        observation.jato_trim,
        _business_powertrain(observation.jato_powertrain),
    )


def _reconciliation_status(
    source_count: int,
    spread_pct: float | None,
    threshold_pct: float,
) -> str:
    if source_count < 2:
        return "single_source"
    if spread_pct is not None and spread_pct > threshold_pct:
        return "conflict"
    return "aligned"


def _reconciliation_action(status: str) -> str:
    if status == "conflict":
        return "review_conflicting_sources"
    if status == "single_source":
        return "add_secondary_source"
    return "keep_current_price"


def _source_observation_payload(
    observation: MsrpObservation,
    source: MsrpSource | None,
) -> dict[str, object]:
    return {
        "observationId": str(observation.observation_id),
        "sourceId": str(observation.source_id),
        "sourceCode": source.source_code if source is not None else None,
        "sourceType": source.source_type if source is not None else None,
        "sourceMsrpValue": float(observation.source_msrp_value),
        "sourceCurrency": observation.source_currency,
        "msrpValue": float(observation.msrp_value),
        "currency": observation.currency,
        "observedAtUtc": observation.observed_at_utc.isoformat(),
        "sourceUrl": observation.source_url,
        "matchStatus": observation.match_status,
        "matchConfidence": float(observation.match_confidence),
        "sourcePayloadHash": observation.source_payload_hash,
    }


def _latest_reconciliation_observations(
    observations: list[MsrpObservation],
) -> list[MsrpObservation]:
    latest_by_source: dict[object, MsrpObservation] = {}
    for observation in sorted(
        observations,
        key=lambda item: item.observed_at_utc,
        reverse=True,
    ):
        latest_by_source.setdefault(observation.source_id, observation)
    return list(latest_by_source.values())


def _reconciliation_metrics(
    latest_observations: list[MsrpObservation],
    threshold_pct: float,
) -> dict[str, object]:
    eur_values = [float(item.msrp_value) for item in latest_observations]
    min_value = min(eur_values) if eur_values else None
    max_value = max(eur_values) if eur_values else None
    avg_value = (
        round(sum(eur_values) / len(eur_values), 2)
        if eur_values
        else None
    )
    spread_value = (
        round(max_value - min_value, 2)
        if min_value is not None and max_value is not None
        else None
    )
    spread_pct = (
        round((spread_value / avg_value) * 100, 2)
        if spread_value is not None and avg_value
        else None
    )
    source_count = len(latest_observations)
    status = _reconciliation_status(
        source_count,
        spread_pct,
        threshold_pct,
    )
    return {
        "sourceCount": source_count,
        "minMsrpValue": min_value,
        "maxMsrpValue": max_value,
        "avgMsrpValue": avg_value,
        "spreadValue": spread_value,
        "spreadPct": spread_pct,
        "status": status,
    }


def _source_observation_payloads(
    latest_observations: list[MsrpObservation],
    source_by_id: dict[object, MsrpSource],
) -> list[dict[str, object]]:
    return [
        _source_observation_payload(
            item,
            source_by_id.get(item.source_id),
        )
        for item in sorted(
            latest_observations,
            key=lambda obs: float(obs.msrp_value),
        )
    ]


def _reconciliation_review_candidates(
    latest_observations: list[MsrpObservation],
    source_by_id: dict[object, MsrpSource],
    metrics: dict[str, object],
    threshold_pct: float,
) -> list[dict[str, object]]:
    source_payloads = _source_observation_payloads(
        latest_observations,
        source_by_id,
    )
    return [
        {
            "candidateType": "source_observation",
            "reconciliationStatus": metrics["status"],
            "recommendedAction": _reconciliation_action(
                str(metrics["status"])
            ),
            "thresholdPct": threshold_pct,
            "spreadPct": metrics["spreadPct"],
            "spreadValue": metrics["spreadValue"],
            "sourceRank": index + 1,
            **payload,
        }
        for index, payload in enumerate(source_payloads)
    ]


def _select_reconciliation_review_observation(
    session: Session,
    key: tuple[str, str, str, str, str],
    latest_observations: list[MsrpObservation],
) -> MsrpObservation:
    current_price = msrp_repo.get_current_price_by_key(
        session,
        key[0],
        key[1],
        key[2],
        key[3],
        key[4],
    )
    if current_price is not None:
        current_observation = msrp_repo.get_observation(
            session,
            current_price.effective_observation_id,
        )
        if current_observation is not None:
            return current_observation
    return sorted(
        latest_observations,
        key=lambda item: item.observed_at_utc,
        reverse=True,
    )[0]


def _build_reconciliation_item(
    session: Session,
    key: tuple[str, str, str, str, str],
    observations: list[MsrpObservation],
    source_by_id: dict[object, MsrpSource],
    threshold_pct: float,
) -> dict[str, object]:
    latest_observations = _latest_reconciliation_observations(observations)
    metrics = _reconciliation_metrics(latest_observations, threshold_pct)
    status = str(metrics["status"])

    current_price = msrp_repo.get_current_price_by_key(
        session,
        key[0],
        key[1],
        key[2],
        key[3],
        key[4],
    )

    return {
        "country": key[0],
        "brand": key[1],
        "jatoModel": key[2],
        "jatoTrim": key[3],
        "jatoPowertrain": key[4] or None,
        "status": status,
        "recommendedAction": _reconciliation_action(status),
        "sourceCount": metrics["sourceCount"],
        "observationCount": len(observations),
        "minMsrpValue": metrics["minMsrpValue"],
        "maxMsrpValue": metrics["maxMsrpValue"],
        "avgMsrpValue": metrics["avgMsrpValue"],
        "spreadValue": metrics["spreadValue"],
        "spreadPct": metrics["spreadPct"],
        "thresholdPct": threshold_pct,
        "currentPrice": (
            current_price_payload(current_price)
            if current_price is not None
            else None
        ),
        "sourceObservations": _source_observation_payloads(
            latest_observations,
            source_by_id,
        ),
    }


def build_multi_source_reconciliation(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    threshold_pct: float = 1.0,
) -> dict[str, object]:
    generated_at = _utc_now()
    observations = msrp_repo.list_reconciliation_observations(
        session,
        country,
        brand,
        jato_model,
        limit,
    )
    sources = msrp_repo.list_sources_by_ids(
        session,
        [item.source_id for item in observations],
    )
    source_by_id = {item.source_id: item for item in sources}

    grouped: dict[tuple[str, str, str, str, str], list[MsrpObservation]] = {}
    for observation in observations:
        grouped.setdefault(_reconciliation_key(observation), []).append(
            observation
        )

    items = [
        _build_reconciliation_item(
            session,
            key,
            group,
            source_by_id,
            threshold_pct,
        )
        for key, group in grouped.items()
    ]
    status_order = {"conflict": 0, "single_source": 1, "aligned": 2}
    items.sort(
        key=lambda item: (
            status_order.get(str(item.get("status")), 99),
            -float(item.get("spreadPct") or 0),
            str(item.get("country") or ""),
            str(item.get("brand") or ""),
            str(item.get("jatoModel") or ""),
        )
    )

    status_counts: dict[str, int] = {}
    for item in items:
        status = str(item.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    return {
        "schemaVersion": "msrp_multi_source_reconciliation_v1",
        "generatedAtUtc": generated_at.isoformat(),
        "filters": {
            "country": country,
            "brand": brand,
            "jatoModel": jato_model,
        },
        "thresholdPct": threshold_pct,
        "summary": {
            "observationRows": len(observations),
            "reconciliationGroupCount": len(items),
            "statusCounts": status_counts,
            "limit": limit,
        },
        "items": items,
    }


def queue_reconciliation_conflicts_for_review(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    threshold_pct: float = 1.0,
    *,
    commit: bool = True,
) -> dict[str, object]:
    generated_at = _utc_now()
    observations = msrp_repo.list_reconciliation_observations(
        session,
        country,
        brand,
        jato_model,
        limit,
    )
    sources = msrp_repo.list_sources_by_ids(
        session,
        [item.source_id for item in observations],
    )
    source_by_id = {item.source_id: item for item in sources}

    grouped: dict[tuple[str, str, str, str, str], list[MsrpObservation]] = {}
    for observation in observations:
        grouped.setdefault(_reconciliation_key(observation), []).append(
            observation
        )

    review_case_rows: list[
        tuple[ReviewCase, MsrpObservation, MsrpSource | None]
    ] = []
    sample_conflicts: list[dict[str, object]] = []
    queued_observation_ids: set[object] = set()
    conflict_group_count = 0
    created_count = 0
    reused_count = 0

    for key, group in grouped.items():
        latest_observations = _latest_reconciliation_observations(group)
        metrics = _reconciliation_metrics(latest_observations, threshold_pct)
        if metrics["status"] != "conflict":
            continue

        conflict_group_count += 1
        review_observation = _select_reconciliation_review_observation(
            session,
            key,
            latest_observations,
        )
        if review_observation.observation_id in queued_observation_ids:
            continue
        queued_observation_ids.add(review_observation.observation_id)

        existing_case = review_repo.get_review_case_by_observation(
            session,
            review_observation.observation_id,
        )
        candidate_matches = _reconciliation_review_candidates(
            latest_observations,
            source_by_id,
            metrics,
            threshold_pct,
        )
        review_case = _ensure_review_case(
            session,
            review_observation,
            candidate_matches,
        )
        if existing_case is None:
            created_count += 1
        else:
            reused_count += 1

        review_case_rows.append(
            (
                review_case,
                review_observation,
                source_by_id.get(review_observation.source_id),
            )
        )
        sample_conflicts.append(
            {
                "country": key[0],
                "brand": key[1],
                "jatoModel": key[2],
                "jatoTrim": key[3],
                "jatoPowertrain": key[4] or None,
                "sourceCount": metrics["sourceCount"],
                "spreadPct": metrics["spreadPct"],
                "spreadValue": metrics["spreadValue"],
                "reviewObservationId": str(
                    review_observation.observation_id
                ),
            }
        )

    _commit_or_conflict(
        session,
        "MSRP reconciliation review case queueing hit a conflict",
        commit=commit,
    )
    return {
        "schemaVersion": "msrp_reconciliation_review_queue_v1",
        "generatedAtUtc": generated_at.isoformat(),
        "filters": {
            "country": country,
            "brand": brand,
            "jatoModel": jato_model,
        },
        "thresholdPct": threshold_pct,
        "summary": {
            "observationRows": len(observations),
            "reconciliationGroupCount": len(grouped),
            "conflictGroupCount": conflict_group_count,
            "reviewCasesQueued": len(review_case_rows),
            "reviewCasesCreated": created_count,
            "reviewCasesReused": reused_count,
            "limit": limit,
        },
        "sampleConflicts": sample_conflicts[:50],
        "sampleReviewCases": [
            review_case_payload(review_case, observation, source)
            for review_case, observation, source in review_case_rows[:50]
        ],
    }


def _normalize_effectiveness_text(value: object | None) -> str:
    return str(value or "").strip().casefold()


def _month_from_iso_datetime(value: object | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return f"{parsed.year:04d}-{parsed.month:02d}"
    except ValueError:
        if len(text) >= 7 and text[4] == "-":
            return text[:7]
    return None


def _offset_month(period: str, offset: int) -> str | None:
    try:
        year = int(period[:4])
        month = int(period[5:7])
    except (ValueError, IndexError):
        return None
    total_months = year * 12 + (month - 1) + offset
    if total_months < 0:
        return None
    next_year, next_month = divmod(total_months, 12)
    return f"{next_year:04d}-{next_month + 1:02d}"


def _window_months(period: str, start_offset: int, count: int) -> list[str]:
    months: list[str] = []
    for offset in range(start_offset, start_offset + max(0, count)):
        shifted = _offset_month(period, offset)
        if shifted is not None:
            months.append(shifted)
    return months


def _month_sales_rows(fact, months: list[str]) -> list[dict[str, object]]:
    if fact is None or len(fact) == 0 or not months:
        return []
    grouped = fact.groupby("period", as_index=False)["sales"].sum()
    sales_by_month = {
        str(row["period"]): float(row["sales"])
        for _, row in grouped.iterrows()
    }
    return [
        {"period": month, "sales": sales_by_month[month]}
        for month in months
        if month in sales_by_month
    ]


def _average_sales(rows: list[dict[str, object]]) -> float | None:
    values = [float(item["sales"]) for item in rows]
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _price_change_direction(alert: dict[str, object]) -> str:
    direction = str(alert.get("direction") or "").strip().lower()
    if direction in {"decrease", "down"}:
        return "down"
    if direction in {"increase", "up"}:
        return "up"
    delta = alert.get("deltaMsrpValue")
    if delta is not None:
        numeric_delta = float(delta)
        if numeric_delta < 0:
            return "down"
        if numeric_delta > 0:
            return "up"
    return "unchanged"


def _effectiveness_label(
    *,
    price_direction: str,
    sales_delta_pct: float | None,
    baseline_count: int,
    post_count: int,
    min_months: int,
) -> str:
    if (
        sales_delta_pct is None
        or baseline_count < min_months
        or post_count < min_months
    ):
        return "insufficient_data"
    threshold = 5.0
    if sales_delta_pct >= threshold:
        return "positive"
    if sales_delta_pct <= -threshold:
        return "negative"
    if price_direction == "unchanged":
        return "neutral"
    return "neutral"


def _filter_fact_for_price_event(fact, alert: dict[str, object]):
    if fact is None or len(fact) == 0:
        return fact
    required = {"model", "period", "sales"}
    if not required.issubset(set(fact.columns)):
        return fact.iloc[0:0]

    model = _normalize_effectiveness_text(alert.get("jatoModel"))
    filtered = fact[
        fact["model"].astype(str).map(_normalize_effectiveness_text) == model
    ]
    brand = _normalize_effectiveness_text(alert.get("brand"))
    if brand and "make" in filtered.columns:
        filtered = filtered[
            filtered["make"].astype(str).map(_normalize_effectiveness_text)
            == brand
        ]
    return filtered


def _build_effectiveness_item(
    *,
    alert: dict[str, object],
    fact,
    generated_at: datetime,
    baseline_window_months: int,
    post_window_months: int,
    post_lag_months: int,
    min_months: int,
) -> dict[str, object]:
    event_month = _month_from_iso_datetime(alert.get("changedAtUtc"))
    price_direction = _price_change_direction(alert)
    country = str(alert.get("country") or "").strip()
    brand = str(alert.get("brand") or "").strip()
    model = str(alert.get("jatoModel") or "").strip()
    trim = str(alert.get("jatoTrim") or "").strip()

    baseline_months = (
        _window_months(
            event_month,
            -baseline_window_months,
            baseline_window_months,
        )
        if event_month
        else []
    )
    post_months = (
        _window_months(event_month, post_lag_months, post_window_months)
        if event_month
        else []
    )
    filtered_fact = _filter_fact_for_price_event(fact, alert)
    baseline_sales = _month_sales_rows(filtered_fact, baseline_months)
    post_sales = _month_sales_rows(filtered_fact, post_months)
    baseline_avg = _average_sales(baseline_sales)
    post_avg = _average_sales(post_sales)

    sales_delta = None
    sales_delta_pct = None
    if baseline_avg is not None and post_avg is not None:
        sales_delta = round(post_avg - baseline_avg, 2)
        if baseline_avg:
            sales_delta_pct = round((sales_delta / baseline_avg) * 100, 2)

    label = _effectiveness_label(
        price_direction=price_direction,
        sales_delta_pct=sales_delta_pct,
        baseline_count=len(baseline_sales),
        post_count=len(post_sales),
        min_months=min_months,
    )
    note = (
        "Model-level sales proxy; JATO monthly sales is not trim-level in this "
        "read model."
    )
    if label == "insufficient_data":
        note = (
            f"Insufficient sales months for comparison: "
            f"baseline={len(baseline_sales)}, post={len(post_sales)}, "
            f"required={min_months}."
        )

    analysis_id = ":".join(
        [
            "msrp-effectiveness",
            _normalize_effectiveness_text(country).replace(" ", "-"),
            _normalize_effectiveness_text(brand).replace(" ", "-"),
            _normalize_effectiveness_text(model).replace(" ", "-"),
            event_month or "unknown-month",
        ]
    )

    return {
        "analysisId": analysis_id,
        "country": country,
        "brand": brand,
        "jatoModel": model,
        "jatoTrim": trim or None,
        "priceEventMonth": event_month,
        "priceChangeDirection": price_direction,
        "priceChangeValue": alert.get("deltaMsrpValue"),
        "priceChangePct": alert.get("deltaPct"),
        "baselineWindowMonths": baseline_months,
        "postWindowMonths": post_months,
        "baselineSales": baseline_sales,
        "postSales": post_sales,
        "baselineAvgSales": baseline_avg,
        "postAvgSales": post_avg,
        "salesDelta": sales_delta,
        "salesDeltaPct": sales_delta_pct,
        "effectivenessLabel": label,
        "confidenceNote": note,
        "generatedAtUtc": generated_at.isoformat(),
        "sourcePriceAlert": alert,
    }


def build_price_sales_effectiveness(
    session: Session,
    country: str | None,
    brand: str | None,
    jato_model: str | None,
    limit: int,
    baseline_window_months: int = 3,
    post_window_months: int = 3,
    post_lag_months: int = 1,
    min_months: int = 1,
    threshold_pct: float = DEFAULT_PRICE_ALERT_THRESHOLD_PCT,
) -> dict[str, object]:
    generated_at = _utc_now()
    price_alerts = list_current_price_alerts(
        session,
        country,
        brand,
        jato_model,
        limit,
        0,
        threshold_pct,
    )
    warnings = [
        item for item in [price_alerts.get("warning")] if item
    ]
    alert_items = [
        item for item in list(price_alerts.get("items") or [])
        if isinstance(item, dict)
    ]

    fact_cache: dict[str, object] = {}
    items: list[dict[str, object]] = []
    for alert in alert_items:
        event_country = str(alert.get("country") or country or "").strip()
        try:
            from app.services import advanced_analysis_service

            if event_country not in fact_cache:
                fact_cache[event_country] = (
                    advanced_analysis_service.build_fact_sales_monthly(
                        country=event_country or None
                    )
                )
            fact = fact_cache[event_country]
        except Exception as exc:
            warning = f"sales_fact_unavailable:{exc}"
            if warning not in warnings:
                warnings.append(warning)
            fact = None
        items.append(
            _build_effectiveness_item(
                alert=alert,
                fact=fact,
                generated_at=generated_at,
                baseline_window_months=baseline_window_months,
                post_window_months=post_window_months,
                post_lag_months=post_lag_months,
                min_months=min_months,
            )
        )

    label_counts: dict[str, int] = {}
    for item in items:
        label = str(item.get("effectivenessLabel") or "unknown")
        label_counts[label] = label_counts.get(label, 0) + 1

    return {
        "schemaVersion": "msrp_price_sales_effectiveness_v1",
        "generatedAtUtc": generated_at.isoformat(),
        "filters": {
            "country": country,
            "brand": brand,
            "jatoModel": jato_model,
        },
        "window": {
            "baselineWindowMonths": baseline_window_months,
            "postWindowMonths": post_window_months,
            "postLagMonths": post_lag_months,
            "minMonths": min_months,
        },
        "summary": {
            "priceEventCount": price_alerts.get("total", len(alert_items)),
            "analyzedEventCount": len(items),
            "labelCounts": label_counts,
            "limit": limit,
        },
        "items": items,
        "warnings": warnings,
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
        observation.jato_powertrain,
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
            current_price.jato_powertrain,
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
    jato_powertrain: str | None,
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
        jato_powertrain,
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
    seen_keys: set[tuple[str, str, str, str, str]] = set()
    price_history_enabled = msrp_repo.has_price_history_table(session)
    for observation in observations:
        business_key = (
            observation.country,
            observation.brand,
            observation.jato_model,
            observation.jato_trim,
            _business_powertrain(observation.jato_powertrain),
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
