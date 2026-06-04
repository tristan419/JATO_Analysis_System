"""API routes for Lease Comparison."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.core.security import UserContext, require_min_role, require_roles
from app.db.session import get_db_session
from app.services.lease_comparison_service import (
    ai_summary,
    create_offer,
    create_compare_set,
    delete_offer,
    get_offer,
    list_compare_sets,
    list_offers,
    solve_cap_cost,
    solve_money_factor,
    solve_monthly_payment,
    solve_residual_value,
    update_offer,
)

router = APIRouter(prefix="/lease-comparison", tags=["lease_comparison"])


# ── Offers CRUD ───────────────────────────────────────────────────


@router.get("/offers")
def get_offers(
    country: str | None = Query(default=None),
    brand: str | None = Query(default=None),
    model: str | None = Query(default=None),
    lease_type: str | None = Query(default=None),
    status: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    return {"offers": list_offers(session, country=country, brand=brand, model_name=model, lease_type=lease_type, status=status)}


@router.post("/offers")
def post_offer(
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("editor", "admin", "order_filler")),
) -> dict:
    try:
        result = create_offer(session, body, user.name)
        session.commit()
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/offers/{offer_id}")
def get_offer_by_id(
    offer_id: str,
    session: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    return get_offer(session, offer_id)


@router.patch("/offers/{offer_id}")
def patch_offer(
    offer_id: str,
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("editor", "admin", "order_filler")),
) -> dict:
    try:
        result = update_offer(session, offer_id, body, user.name)
        session.commit()
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/offers/{offer_id}")
def delete_offer_by_id(
    offer_id: str,
    session: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("admin")),
) -> dict:
    try:
        result = delete_offer(session, offer_id)
        session.commit()
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# ── Parameter Solver (stateless) ─────────────────────────────────


@router.post("/offers/solve")
def solve_offer(
    body: dict,
    _: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    """Solve for an unknown lease parameter."""
    solve_for = body.get("solveFor", "monthly_payment")
    try:
        if solve_for == "monthly_payment":
            result = solve_monthly_payment(
                float(body["capCost"]), float(body["residualValue"]),
                int(body.get("termMonths", 36)), float(body.get("moneyFactor", 0)),
            )
            return {"monthlyPayment": result}

        if solve_for == "money_factor":
            return solve_money_factor(
                float(body["monthlyPayment"]), float(body["capCost"]),
                float(body["residualValue"]), int(body.get("termMonths", 36)),
            )

        if solve_for == "cap_cost":
            result = solve_cap_cost(
                float(body["monthlyPayment"]), float(body["residualValue"]),
                int(body.get("termMonths", 36)), float(body.get("moneyFactor", 0)),
            )
            return {"capCost": result}

        if solve_for == "residual_value":
            result = solve_residual_value(
                float(body["monthlyPayment"]), float(body["capCost"]),
                int(body.get("termMonths", 36)), float(body.get("moneyFactor", 0)),
            )
            return {"residualValue": result, "residualValuePercent": round(result / float(body["capCost"]) * 100, 2)}

        raise HTTPException(status_code=400, detail=f"Unknown solveFor: {solve_for}")
    except (KeyError, ValueError, ZeroDivisionError) as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── Compare Sets ─────────────────────────────────────────────────


@router.post("/compare-sets")
def post_compare_set(
    body: dict,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_roles("editor", "admin", "order_filler")),
) -> dict:
    result = create_compare_set(session, body, user.name)
    session.commit()
    return result


@router.get("/compare-sets")
def get_compare_sets(
    country: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    _: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    return {"compareSets": list_compare_sets(session, country)}


# ── AI Summary ───────────────────────────────────────────────────


@router.post("/ai-summary")
def post_ai_summary(
    body: dict,
    _: UserContext = Depends(require_min_role("viewer")),
) -> dict:
    offers = body.get("offers", [])
    if not offers:
        raise HTTPException(status_code=400, detail="No offers provided")
    summary = ai_summary(offers)
    return {"summary": summary}
