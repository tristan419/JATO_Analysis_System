"""Lease Comparison — lease math, CRUD, and AI summary."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from urllib.request import Request, urlopen

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.db.models import LeaseCompareSet, LeaseOffer, LeaseOfferVersion
from app.infra import lease_comparison_repository as repo
from app.services.fx_service import convert_amount_to_eur


# ── Lease math (stateless, pure functions) ──────────────────────


def mf_to_apr(mf: float) -> float:
    return round(mf * 2400, 4)


def apr_to_mf(apr: float) -> float:
    return round(apr / 2400, 8)


def solve_monthly_payment(cap_cost: float, rv: float, months: int, mf: float) -> float:
    """P = (C - R) / M + (C + R) * MF"""
    return round((cap_cost - rv) / months + (cap_cost + rv) * mf, 2)


def solve_money_factor(monthly_payment: float, cap_cost: float, rv: float, months: int) -> dict:
    """Reverse-calculate MF and APR from known monthly payment."""
    if (cap_cost + rv) == 0:
        raise ValueError("Cap Cost + RV cannot be zero")
    mf = (monthly_payment - (cap_cost - rv) / months) / (cap_cost + rv)
    return {"money_factor": round(mf, 8), "apr_percent": mf_to_apr(mf), "apr_source": "reverse_calculated"}


def solve_cap_cost(monthly_payment: float, rv: float, months: int, mf: float) -> float:
    """Reverse-calculate Cap Cost from known monthly payment."""
    denom = 1.0 / months + mf
    if denom == 0:
        raise ValueError("Denominator is zero")
    return round((monthly_payment - rv * (mf - 1.0 / months)) / denom, 2)


def solve_residual_value(monthly_payment: float, cap_cost: float, months: int, mf: float) -> float:
    """Reverse-calculate Residual Value from known monthly payment."""
    denom = mf - 1.0 / months
    if denom == 0:
        raise ValueError("Denominator is zero")
    return round((monthly_payment - cap_cost * (1.0 / months + mf)) / denom, 2)


def effective_monthly(monthly_payment: float, months: int, upfront: float, treatment: str) -> float:
    """Effective monthly equivalent accounting for upfront costs."""
    if treatment == "refundable_deposit":
        return monthly_payment
    return round((monthly_payment * months + upfront) / months, 2)


def compute_risk_level(offer: LeaseOffer) -> str:
    flags = 0
    if not offer.rv_guaranteed: flags += 1
    if not offer.service_included: flags += 1
    if offer.apr_source == "reverse_calculated": flags += 1
    if not offer.vat_included: flags += 1
    if flags <= 1: return "low"
    if flags <= 3: return "medium"
    return "high"


# ── CRUD ──────────────────────────────────────────────────────────


def list_offers(
    session: Session,
    country: str | None = None,
    brand: str | None = None,
    model_name: str | None = None,
    lease_type: str | None = None,
    status: str | None = None,
) -> list[dict]:
    offers = repo.list_offers(session, country=country, brand=brand, model_name=model_name,
                              lease_type=lease_type, status=status)
    return [_offer_to_dict(o) for o in offers]


def get_offer(session: Session, offer_id: str) -> dict:
    offer = repo.get_offer_by_id(session, offer_id)
    if not offer:
        raise HTTPException(status_code=404, detail="Offer not found")
    versions = repo.list_versions(session, offer_id)
    result = _offer_to_dict(offer)
    result["versions"] = [
        {"versionId": str(v.version_id), "versionNo": v.version_no,
         "changedBy": v.changed_by, "changeReason": v.change_reason,
         "changedAt": v.updated_at_utc.isoformat() if v.updated_at_utc else None}
        for v in versions
    ]
    return result


def create_offer(session: Session, payload: dict, username: str) -> dict:
    offer = _build_offer_from_payload(payload, username)
    _apply_fx(session, offer, payload.get("currency", "EUR"))
    offer.risk_level = compute_risk_level(offer)
    repo.add_offer(session, offer)
    return _offer_to_dict(offer)


def update_offer(session: Session, offer_id: str, payload: dict, username: str) -> dict:
    offer = repo.get_offer_by_id(session, offer_id)
    if not offer:
        raise HTTPException(status_code=404, detail="Offer not found")

    # Save version snapshot before mutation
    version_no = (repo.max_version_no(session, offer_id) or 0) + 1
    snapshot = _offer_to_dict(offer)
    version = LeaseOfferVersion(
        offer_id=offer.offer_id, version_no=version_no,
        snapshot_json=snapshot, change_reason=payload.get("changeReason"),
        changed_by=username,
    )
    repo.add_version(session, version)

    _apply_offer_updates(offer, payload, username)
    _apply_fx(session, offer, payload.get("currency", offer.currency))
    offer.risk_level = compute_risk_level(offer)
    offer.row_version += 1
    session.flush()
    return _offer_to_dict(offer)


def delete_offer(session: Session, offer_id: str) -> dict:
    offer = repo.get_offer_by_id(session, offer_id)
    if not offer:
        raise HTTPException(status_code=404, detail="Offer not found")
    repo.delete_offer(session, offer)
    return {"offerId": offer_id, "deleted": True}


def create_compare_set(session: Session, payload: dict, username: str) -> dict:
    cs = LeaseCompareSet(
        name=payload["name"],
        country_code=payload.get("countryCode"),
        selected_offer_ids=payload.get("selectedOfferIds") or [],
        created_by=username,
    )
    repo.add_compare_set(session, cs)
    return _compare_set_to_dict(cs)


def list_compare_sets(session: Session, country: str | None = None) -> list[dict]:
    sets = repo.list_compare_sets(session, country)
    return [_compare_set_to_dict(s) for s in sets]


# ── AI Summary ────────────────────────────────────────────────────


def ai_summary(offers: list[dict]) -> str:
    api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        return "AI summary unavailable: DeepSeek API key not configured."

    prompt = _build_ai_prompt(offers)
    body = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": "You are a lease comparison analyst. Output concise business analysis in Chinese. Structure: 1. Executive Summary (one sentence). 2. Key Drivers (bullet points). 3. Risk Notes (bullet points). 4. Recommended Action (bullet points)."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.3,
        "max_tokens": 1000,
        "stream": False,
    }
    try:
        req = Request(
            "https://api.deepseek.com/chat/completions",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            method="POST",
        )
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        return f"AI summary failed: {e}"


# ── Helpers ───────────────────────────────────────────────────────


def _build_offer_from_payload(payload: dict, username: str) -> LeaseOffer:
    offer = LeaseOffer(created_by=username, updated_by=username)
    _apply_offer_updates(offer, payload, username)
    return offer


def _apply_offer_updates(offer: LeaseOffer, payload: dict, username: str) -> None:
    for attr, key in {
        "country_code": "countryCode", "currency": "currency",
        "brand": "brand", "model_name": "modelName", "version": "version",
        "powertrain": "powertrain", "segment": "segment", "lease_type": "leaseType",
        "provider": "provider", "status": "status",
        "term_months": "termMonths", "mileage_per_year": "mileagePerYear",
        "rv_guaranteed": "rvGuaranteed", "service_included": "serviceIncluded",
        "insurance_included": "insuranceIncluded", "tyre_included": "tyreIncluded",
        "vat_included": "vatIncluded",
        "deposit_required": "depositRequired", "deposit_refundable": "depositRefundable",
        "source_type": "sourceType", "source_url": "sourceUrl",
        "notes": "notes", "upfront_treatment": "upfrontTreatment",
        "apr_source": "aprSource",
    }.items():
        if key in payload:
            setattr(offer, attr, payload[key])

    # Numeric fields
    for attr, key in {
        "monthly_payment": "monthlyPayment", "down_payment": "downPayment",
        "upfront_amount": "upfrontAmount", "cap_cost": "capCost",
        "residual_value": "residualValue", "residual_value_percent": "residualValuePercent",
        "apr_percent": "aprPercent", "money_factor": "moneyFactor",
        "fx_rate_to_eur": "fxRateToEur",
    }.items():
        if key in payload and payload[key] is not None:
            setattr(offer, attr, float(payload[key]))

    # Date fields
    for attr, key in {
        "effective_date": "effectiveDate", "expiry_date": "expiryDate",
        "fx_rate_date": "fxRateDate",
    }.items():
        if key in payload and payload[key]:
            setattr(offer, attr, date.fromisoformat(str(payload[key])[:10]))

    offer.updated_by = username


def _apply_fx(session: Session, offer: LeaseOffer, currency: str) -> None:
    """Convert original-currency amounts to EUR using fx_service."""
    if not currency or currency.upper() == "EUR":
        offer.monthly_payment_eur = offer.monthly_payment
        offer.cap_cost_eur = offer.cap_cost
        offer.residual_value_eur = offer.residual_value
        offer.down_payment_eur = offer.down_payment
    elif offer.fx_rate_to_eur:
        if offer.monthly_payment:
            offer.monthly_payment_eur = round(offer.monthly_payment * offer.fx_rate_to_eur, 2)
        if offer.cap_cost:
            offer.cap_cost_eur = round(offer.cap_cost * offer.fx_rate_to_eur, 2)
        if offer.residual_value:
            offer.residual_value_eur = round(offer.residual_value * offer.fx_rate_to_eur, 2)
        if offer.down_payment:
            offer.down_payment_eur = round(offer.down_payment * offer.fx_rate_to_eur, 2)
    else:
        try:
            if offer.monthly_payment:
                offer.monthly_payment_eur, _ = convert_amount_to_eur(offer.monthly_payment, currency)
            if offer.cap_cost:
                offer.cap_cost_eur, _ = convert_amount_to_eur(offer.cap_cost, currency)
            if offer.residual_value:
                offer.residual_value_eur, _ = convert_amount_to_eur(offer.residual_value, currency)
            if offer.down_payment:
                offer.down_payment_eur, _ = convert_amount_to_eur(offer.down_payment, currency)
        except Exception:
            pass

    # Effective monthly
    mp = offer.monthly_payment_eur or 0
    months = offer.term_months or 36
    upfront = offer.upfront_amount or 0
    treatment = offer.upfront_treatment or ""
    offer.effective_monthly_eur = effective_monthly(mp, months, upfront, treatment)

    # Total contract cost
    offer.total_contract_cost_eur = round(mp * months + (upfront if treatment != "refundable_deposit" else 0), 2)


def _offer_to_dict(o: LeaseOffer) -> dict:
    return {
        "offerId": str(o.offer_id), "countryCode": o.country_code, "currency": o.currency,
        "brand": o.brand, "modelName": o.model_name, "version": o.version,
        "powertrain": o.powertrain, "segment": o.segment, "leaseType": o.lease_type,
        "provider": o.provider, "status": o.status,
        "fxRateToEur": float(o.fx_rate_to_eur) if o.fx_rate_to_eur else None,
        "fxRateDate": o.fx_rate_date.isoformat() if o.fx_rate_date else None,
        "fxSource": o.fx_source, "fxLocked": o.fx_locked,
        "monthlyPayment": float(o.monthly_payment) if o.monthly_payment else None,
        "monthlyPaymentEur": float(o.monthly_payment_eur) if o.monthly_payment_eur else None,
        "effectiveMonthlyEur": float(o.effective_monthly_eur) if o.effective_monthly_eur else None,
        "downPayment": float(o.down_payment) if o.down_payment else None,
        "downPaymentEur": float(o.down_payment_eur) if o.down_payment_eur else None,
        "upfrontAmount": float(o.upfront_amount) if o.upfront_amount else None,
        "upfrontTreatment": o.upfront_treatment,
        "termMonths": o.term_months, "mileagePerYear": o.mileage_per_year,
        "capCost": float(o.cap_cost) if o.cap_cost else None,
        "capCostEur": float(o.cap_cost_eur) if o.cap_cost_eur else None,
        "residualValue": float(o.residual_value) if o.residual_value else None,
        "residualValueEur": float(o.residual_value_eur) if o.residual_value_eur else None,
        "residualValuePercent": float(o.residual_value_percent) if o.residual_value_percent else None,
        "aprPercent": float(o.apr_percent) if o.apr_percent else None,
        "moneyFactor": float(o.money_factor) if o.money_factor else None,
        "aprSource": o.apr_source,
        "rvGuaranteed": o.rv_guaranteed, "serviceIncluded": o.service_included,
        "insuranceIncluded": o.insurance_included, "tyreIncluded": o.tyre_included,
        "vatIncluded": o.vat_included,
        "depositRequired": o.deposit_required, "depositRefundable": o.deposit_refundable,
        "sourceType": o.source_type, "sourceUrl": o.source_url,
        "effectiveDate": o.effective_date.isoformat() if o.effective_date else None,
        "expiryDate": o.expiry_date.isoformat() if o.expiry_date else None,
        "totalContractCostEur": float(o.total_contract_cost_eur) if o.total_contract_cost_eur else None,
        "riskLevel": o.risk_level, "notes": o.notes,
        "createdBy": o.created_by, "updatedBy": o.updated_by,
        "createdAt": o.created_at_utc.isoformat() if o.created_at_utc else None,
        "updatedAt": o.updated_at_utc.isoformat() if o.updated_at_utc else None,
    }


def _compare_set_to_dict(s: LeaseCompareSet) -> dict:
    return {
        "compareId": str(s.compare_id), "name": s.name,
        "countryCode": s.country_code,
        "selectedOfferIds": s.selected_offer_ids,
        "createdBy": s.created_by,
        "createdAt": s.created_at_utc.isoformat() if s.created_at_utc else None,
    }


def _build_ai_prompt(offers: list[dict]) -> str:
    lines = ["Compare these lease offers:\n"]
    for i, o in enumerate(offers):
        lines.append(f"Offer {i + 1}: {o.get('brand')} {o.get('modelName')} {o.get('version','')} | "
                      f"Lease: {o.get('leaseType')} | Provider: {o.get('provider','N/A')} | "
                      f"Monthly: {o.get('monthlyPaymentEur','?')} EUR | "
                      f"Down: {o.get('downPaymentEur','?')} EUR | "
                      f"Term: {o.get('termMonths','?')}mo | "
                      f"Cap Cost: {o.get('capCostEur','?')} EUR | "
                      f"RV: {o.get('residualValueEur','?')} EUR ({o.get('residualValuePercent','?')}%) | "
                      f"APR: {o.get('aprPercent','?')}% ({o.get('aprSource','manual')}) | "
                      f"RV Guaranteed: {o.get('rvGuaranteed')} | "
                      f"Service: {o.get('serviceIncluded')} | VAT: {o.get('vatIncluded')}")
    return "\n".join(lines)
