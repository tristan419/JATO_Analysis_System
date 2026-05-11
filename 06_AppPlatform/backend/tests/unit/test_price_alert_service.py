"""Tests for price alert service."""

from __future__ import annotations

from app.copilot_governance.price_alert_service import (
    check_price_alerts,
    estimate_monthly_payments,
    analyze_price_trend,
)


class TestPriceAlerts:
    def test_high_price_alert(self):
        alerts = check_price_alerts(vehicle_price=70_000, brand="Test", model="X", currency="EUR")
        assert any(a.severity == "warning" for a in alerts)

    def test_low_price_alert(self):
        alerts = check_price_alerts(vehicle_price=15_000, brand="Test", model="X", currency="EUR")
        assert any(a.alert_type == "price_level" for a in alerts)

    def test_zero_price_no_alerts(self):
        alerts = check_price_alerts()
        assert len(alerts) == 0


class TestMonthlyPayments:
    def test_36_month(self):
        estimates = estimate_monthly_payments(vehicle_price=40_000, currency="EUR")
        assert len(estimates) == 3
        assert estimates[0].term_months == 36
        assert estimates[0].monthly_payment > 800

    def test_zero_price_empty(self):
        estimates = estimate_monthly_payments()
        assert len(estimates) == 0


class TestPriceTrendSummary:
    def test_basic_summary(self):
        summary = analyze_price_trend(
            country="Sweden", brand="Volvo", model="XC60",
            vehicle_price=550_000, currency="SEK",
        )
        assert summary.country == "Sweden"
        assert summary.monthly_estimates
        assert summary.trend_direction
