"""Price Alert Service — EUR-unified MSRP monitoring via msrp_repository.

All prices are unified in EUR. Currency conversion is handled by the MSRP
module (fx_rate_to_eur field on CurrentPrice / PriceHistory).
"""

from __future__ import annotations

from pydantic import BaseModel, Field

_UNIFIED_CURRENCY = "EUR"
_WARNING_CHANGE_PCT = 3.0
_CRITICAL_CHANGE_PCT = 10.0


class PriceAlert(BaseModel):
    alert_type: str
    severity: str = "info"
    title: str
    detail: str
    change_pct: float | None = None


class MonthlyPaymentEstimate(BaseModel):
    term_months: int
    monthly_payment_eur: float
    total_cost_eur: float
    interest_rate_pct: float
    down_payment_pct: float = 20.0


class PriceTrendSummary(BaseModel):
    country: str
    brand: str = ""
    model: str = ""
    alerts: list[PriceAlert] = Field(default_factory=list)
    monthly_estimates: list[MonthlyPaymentEstimate] = Field(
        default_factory=list
    )
    latest_price_eur: float | None = None
    previous_price_eur: float | None = None
    price_change_pct: float | None = None
    trend_direction: str = "stable"
    source: str = "msrp_repository"


def check_price_alerts(
    country: str = "",
    brand: str = "",
    model: str = "",
    vehicle_price_eur: float = 0,
) -> list[PriceAlert]:
    alerts: list[PriceAlert] = []
    if vehicle_price_eur <= 0:
        return alerts

    label = f"{brand} {model}".strip()

    if vehicle_price_eur > 60_000:
        alerts.append(
            PriceAlert(
                alert_type="price_level",
                severity="warning",
                title="高价位预警",
                detail=(
                    f"{label} EUR {vehicle_price_eur:,.0f} 超过 60k 阈值。"
                    "建议竞品对比和金融方案分析。"
                ),
            )
        )
    elif vehicle_price_eur > 40_000:
        alerts.append(
            PriceAlert(
                alert_type="price_level",
                severity="info",
                title="中高价位提示",
                detail=(
                    f"{label} EUR {vehicle_price_eur:,.0f}，"
                    "处于中高价格区间。"
                ),
            )
        )
    elif vehicle_price_eur < 20_000:
        alerts.append(
            PriceAlert(
                alert_type="price_level",
                severity="info",
                title="低价位机会",
                detail=(
                    f"{label} EUR {vehicle_price_eur:,.0f} 低于市场均价，"
                    "存在价格优势机会。"
                ),
            )
        )

    return alerts


def estimate_monthly_payments(
    vehicle_price_eur: float = 0,
    down_payment_pct: float = 20.0,
    interest_rates: dict[int, float] | None = None,
) -> list[MonthlyPaymentEstimate]:
    if vehicle_price_eur <= 0:
        return []

    rates = interest_rates or {36: 3.5, 48: 4.5, 60: 5.5}
    estimates: list[MonthlyPaymentEstimate] = []
    down = vehicle_price_eur * down_payment_pct / 100
    financed = vehicle_price_eur - down

    for months, rate_pct in rates.items():
        monthly_rate = rate_pct / 100 / 12
        if monthly_rate > 0:
            payment = (
                financed
                * monthly_rate
                * (1 + monthly_rate) ** months
                / ((1 + monthly_rate) ** months - 1)
            )
        else:
            payment = financed / months
        estimates.append(
            MonthlyPaymentEstimate(
                term_months=months,
                monthly_payment_eur=round(payment, 0),
                total_cost_eur=round(payment * months + down, 0),
                interest_rate_pct=rate_pct,
                down_payment_pct=down_payment_pct,
            )
        )

    return estimates


def _build_price_change_alert(
    *,
    brand: str,
    model: str,
    latest_price_eur: float,
    previous_price_eur: float,
    change_pct: float,
) -> PriceAlert:
    abs_change = abs(change_pct)
    severity = "info"
    if abs_change >= _CRITICAL_CHANGE_PCT:
        severity = "critical"
    elif abs_change >= _WARNING_CHANGE_PCT:
        severity = "warning"

    direction = "上调" if change_pct > 0 else "下调"
    label = f"{brand} {model}".strip()
    return PriceAlert(
        alert_type="price_change",
        severity=severity,
        title=f"MSRP {direction}",
        detail=(
            f"{label} MSRP 从 EUR {previous_price_eur:,.0f} "
            f"{direction}至 EUR {latest_price_eur:,.0f}，"
            f"变化 {change_pct:+.1f}%。"
        ),
        change_pct=round(change_pct, 1),
    )


def analyze_price_trend(
    country: str = "",
    brand: str = "",
    model: str = "",
    vehicle_price_eur: float = 0,
) -> PriceTrendSummary:
    alerts = check_price_alerts(
        country=country, brand=brand, model=model,
        vehicle_price_eur=vehicle_price_eur,
    )

    monthly = estimate_monthly_payments(vehicle_price_eur=vehicle_price_eur)

    trend = "stable"
    if vehicle_price_eur > 60_000:
        trend = "premium"
    elif vehicle_price_eur < 25_000:
        trend = "value"

    return PriceTrendSummary(
        country=country, brand=brand, model=model,
        alerts=alerts, monthly_estimates=monthly,
        latest_price_eur=vehicle_price_eur if vehicle_price_eur > 0 else None,
        trend_direction=trend,
    )


def query_msrp_price_history(
    country: str = "",
    brand: str = "",
    model: str = "",
    trim: str = "",
    powertrain: str | None = None,
    limit: int = 24,
) -> PriceTrendSummary:
    """Query MSRP repository for actual price history data in EUR."""
    try:
        from app.db.session import get_session_factory
        from app.infra import msrp_repository

        session = get_session_factory()()
        try:
            records = msrp_repository.list_price_history(
                session,
                country=country or None,
                brand=brand if brand else None,
                jato_model=model if model else None,
                jato_trim=trim if trim else None,
                jato_powertrain=powertrain,
                limit=limit,
            )
            if not records:
                return PriceTrendSummary(
                    country=country, brand=brand, model=model,
                    alerts=[PriceAlert(
                        alert_type="no_data", severity="info",
                        title="无价格历史",
                        detail=f"MSRP 数据库中无 {brand} {model} 的历史价格记录。",
                    )],
                )

            latest = records[0]
            price_eur = float(latest.msrp_value or 0)
            prev_price = (
                float(records[1].msrp_value or 0)
                if len(records) > 1
                else price_eur
            )

            alerts = check_price_alerts(
                country=country,
                brand=brand,
                model=model,
                vehicle_price_eur=price_eur,
            )
            monthly = estimate_monthly_payments(vehicle_price_eur=price_eur)

            change_pct = (
                ((price_eur - prev_price) / prev_price * 100)
                if prev_price > 0
                else 0
            )
            if len(records) > 1 and change_pct != 0:
                alerts.append(
                    _build_price_change_alert(
                        brand=brand,
                        model=model,
                        latest_price_eur=price_eur,
                        previous_price_eur=prev_price,
                        change_pct=change_pct,
                    )
                )

            return PriceTrendSummary(
                country=country, brand=brand, model=model,
                alerts=alerts, monthly_estimates=monthly,
                latest_price_eur=price_eur,
                previous_price_eur=prev_price,
                price_change_pct=round(change_pct, 1),
                trend_direction=(
                    "up"
                    if change_pct > 1
                    else "down"
                    if change_pct < -1
                    else "stable"
                ),
                source="msrp_repository",
            )
        finally:
            session.close()
    except Exception as exc:
        return PriceTrendSummary(
            country=country, brand=brand, model=model,
            alerts=[
                PriceAlert(
                    alert_type="error",
                    severity="warning",
                    title="MSRP 查询失败",
                    detail=f"无法查询价格历史: {exc}",
                )
            ],
        )
