"""Price Alert Service — MSRP price history, monthly payment estimates, policy alerts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PriceAlert(BaseModel):
    alert_type: str
    severity: str = "info"
    title: str
    detail: str
    currency: str = ""
    change_pct: float | None = None


class MonthlyPaymentEstimate(BaseModel):
    term_months: int
    monthly_payment: float
    total_cost: float
    interest_rate_pct: float
    down_payment_pct: float = 20.0
    currency: str = ""


class PriceTrendSummary(BaseModel):
    country: str
    brand: str = ""
    model: str = ""
    currency: str = ""
    alerts: list[PriceAlert] = Field(default_factory=list)
    monthly_estimates: list[MonthlyPaymentEstimate] = Field(default_factory=list)
    latest_price: float | None = None
    price_3mo_ago: float | None = None
    price_12mo_ago: float | None = None
    trend_direction: str = "stable"


def check_price_alerts(
    country: str = "",
    brand: str = "",
    model: str = "",
    vehicle_price: float = 0,
    currency: str = "",
) -> list[PriceAlert]:
    alerts: list[PriceAlert] = []
    if vehicle_price <= 0:
        return alerts

    # Price level alerts
    if vehicle_price > 60_000:
        alerts.append(PriceAlert(
            alert_type="price_level",
            severity="warning",
            title=f"高价位预警",
            detail=f"{brand} {model} 价格 {vehicle_price:,.0f} {currency}，超过 60,000 阈值。考虑竞品对比和金融方案。",
            currency=currency,
        ))

    if vehicle_price < 20_000:
        alerts.append(PriceAlert(
            alert_type="price_level",
            severity="info",
            title=f"低价位机会",
            detail=f"{brand} {model} 价格 {vehicle_price:,.0f} {currency}，低于市场均价。存在价格优势机会。",
            currency=currency,
        ))

    return alerts


def estimate_monthly_payments(
    vehicle_price: float = 0,
    currency: str = "",
    down_payment_pct: float = 20.0,
    interest_rates: dict[int, float] | None = None,
) -> list[MonthlyPaymentEstimate]:
    if vehicle_price <= 0:
        return []

    rates = interest_rates or {36: 3.5, 48: 4.5, 60: 5.5}
    estimates: list[MonthlyPaymentEstimate] = []
    down = vehicle_price * down_payment_pct / 100
    financed = vehicle_price - down

    for months, rate_pct in rates.items():
        monthly_rate = rate_pct / 100 / 12
        if monthly_rate > 0:
            payment = financed * monthly_rate * (1 + monthly_rate) ** months / ((1 + monthly_rate) ** months - 1)
        else:
            payment = financed / months
        estimates.append(MonthlyPaymentEstimate(
            term_months=months,
            monthly_payment=round(payment, 0),
            total_cost=round(payment * months + down, 0),
            interest_rate_pct=rate_pct,
            down_payment_pct=down_payment_pct,
            currency=currency,
        ))

    return estimates


def analyze_price_trend(
    country: str = "",
    brand: str = "",
    model: str = "",
    vehicle_price: float = 0,
    currency: str = "",
) -> PriceTrendSummary:
    alerts = check_price_alerts(
        country=country, brand=brand, model=model,
        vehicle_price=vehicle_price, currency=currency,
    )

    monthly = estimate_monthly_payments(
        vehicle_price=vehicle_price, currency=currency,
    )

    trend = "stable"
    if vehicle_price > 60_000:
        trend = "premium"

    return PriceTrendSummary(
        country=country, brand=brand, model=model, currency=currency,
        alerts=alerts, monthly_estimates=monthly,
        latest_price=vehicle_price if vehicle_price > 0 else None,
        trend_direction=trend,
    )
