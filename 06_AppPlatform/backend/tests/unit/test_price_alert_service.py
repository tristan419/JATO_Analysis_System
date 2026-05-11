"""Tests for price alert service."""

from __future__ import annotations

from app.copilot_governance.price_alert_service import (
    check_price_alerts,
    estimate_monthly_payments,
    analyze_price_trend,
    query_msrp_price_history,
)


class TestPriceAlerts:
    def test_high_price_alert(self):
        alerts = check_price_alerts(vehicle_price_eur=70_000, brand="Test", model="X")
        assert any(a.severity == "warning" for a in alerts)

    def test_low_price_alert(self):
        alerts = check_price_alerts(vehicle_price_eur=15_000, brand="Test", model="X")
        assert any(a.alert_type == "price_level" for a in alerts)

    def test_zero_price_no_alerts(self):
        alerts = check_price_alerts()
        assert len(alerts) == 0


class TestMonthlyPayments:
    def test_36_month(self):
        estimates = estimate_monthly_payments(vehicle_price_eur=40_000)
        assert len(estimates) == 3
        assert estimates[0].term_months == 36
        assert estimates[0].monthly_payment_eur > 800

    def test_zero_price_empty(self):
        estimates = estimate_monthly_payments()
        assert len(estimates) == 0


class TestPriceTrendSummary:
    def test_basic_summary(self):
        summary = analyze_price_trend(
            country="Sweden", brand="Volvo", model="XC60",
            vehicle_price_eur=52_000,
        )
        assert summary.country == "Sweden"
        assert summary.monthly_estimates
        assert summary.trend_direction

    def test_euro_is_unified(self):
        summary = analyze_price_trend(vehicle_price_eur=45_000)
        assert summary.latest_price_eur == 45_000
        for est in summary.monthly_estimates:
            assert isinstance(est.monthly_payment_eur, float)

    def test_query_msrp_no_db_handles_gracefully(self):
        summary = query_msrp_price_history(country="Sweden", brand="Volvo", model="XC60")
        assert summary is not None
        assert summary.source == "msrp_repository"
