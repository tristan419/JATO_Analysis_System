"""Tests for per-country tax calculators."""

from __future__ import annotations

import pytest

from app.copilot_governance.tax_calculator import (
    calculate_country_taxes,
    calculate_sweden_taxes,
    calculate_norway_taxes,
    calculate_finland_taxes,
    calculate_denmark_taxes,
    calculate_germany_taxes,
    calculate_netherlands_taxes,
    supported_countries,
)


class TestSweden:
    def test_malus_for_high_co2_ice(self):
        est = calculate_sweden_taxes(co2_gkm=120, powertrain="ICE")
        assert est.total_annual > 5000  # 132 * (120-70) = 6600
        assert any("Malus" in c.label for c in est.annual_costs)

    def test_bev_exempt_from_malus(self):
        est = calculate_sweden_taxes(co2_gkm=0, powertrain="BEV")
        assert any("exempt" in e.lower() for e in est.exemptions)

    def test_low_co2_base_rate(self):
        est = calculate_sweden_taxes(co2_gkm=50, powertrain="PHEV")
        assert est.total_annual <= 360 + 100  # base rate only

    def test_company_car_bev_reduction(self):
        est = calculate_sweden_taxes(
            co2_gkm=0, powertrain="BEV", vehicle_price=500_000, is_company_car=True,
        )
        assert any("förmån" in c.label.lower() for c in est.annual_costs)

    def test_3year_total(self):
        est = calculate_sweden_taxes(co2_gkm=120, powertrain="ICE")
        assert est.total_annual > 0
        assert est.total_3year == pytest.approx(est.total_annual * 3, rel=0.1)


class TestNorway:
    def test_bev_vat_exemption(self):
        est = calculate_norway_taxes(
            co2_gkm=0, powertrain="BEV", vehicle_price=400_000,
        )
        assert any("增值税" in c.label or "VAT" in c.label for c in est.one_time_costs)
        assert any("exempt" in e.lower() or "豁免" in e for e in est.exemptions)

    def test_weight_tax_for_heavy_vehicle(self):
        est = calculate_norway_taxes(weight_kg=2000, powertrain="ICE")
        assert any("重量" in c.label for c in est.annual_costs)

    def test_weight_tax_below_threshold(self):
        est = calculate_norway_taxes(weight_kg=400, powertrain="ICE")
        assert not any("重量" in c.label for c in est.annual_costs)

    def test_bev_toll_subsidy(self):
        est = calculate_norway_taxes(powertrain="BEV")
        assert any("过路费" in s.label for s in est.subsidies)


class TestFinland:
    def test_car_tax_high_co2(self):
        est = calculate_finland_taxes(co2_gkm=200, vehicle_price=40_000, powertrain="ICE")
        assert any("Autovero" in c.label for c in est.one_time_costs)
        assert est.total_one_time > 5000

    def test_bev_minimum_rate(self):
        est = calculate_finland_taxes(co2_gkm=0, vehicle_price=40_000, powertrain="BEV")
        auto_costs = [c for c in est.one_time_costs if "Autovero" in c.label]
        assert auto_costs[0].amount <= 40_000 * 0.03  # ~2.7%

    def test_bev_subsidy(self):
        est = calculate_finland_taxes(co2_gkm=0, vehicle_price=40_000, powertrain="BEV")
        assert any("补贴" in s.label for s in est.subsidies)


class TestDenmark:
    def test_high_price_150pct_band(self):
        est = calculate_denmark_taxes(vehicle_price=300_000, powertrain="ICE")
        assert est.total_one_time > 50_000  # progressive

    def test_bev_deduction(self):
        est = calculate_denmark_taxes(vehicle_price=300_000, powertrain="BEV")
        ice = calculate_denmark_taxes(vehicle_price=300_000, powertrain="ICE")
        assert est.total_one_time < ice.total_one_time  # BEV cheaper

    def test_green_tax_bev_lowest(self):
        est_bev = calculate_denmark_taxes(powertrain="BEV")
        est_ice = calculate_denmark_taxes(co2_gkm=150, powertrain="ICE")
        bev_green = sum(c.amount for c in est_bev.annual_costs if "Grøn" in c.label)
        ice_green = sum(c.amount for c in est_ice.annual_costs if "Grøn" in c.label)
        assert bev_green < ice_green


class TestGermany:
    def test_bev_10yr_exemption(self):
        est = calculate_germany_taxes(powertrain="BEV")
        assert any("exempt" in e.lower() for e in est.exemptions)

    def test_co2_surcharge(self):
        est = calculate_germany_taxes(co2_gkm=150, powertrain="ICE")
        assert any("CO₂" in c.label for c in est.annual_costs)

    def test_company_car_bev_benefit(self):
        est = calculate_germany_taxes(
            powertrain="BEV", vehicle_price=60_000, is_company_car=True,
        )
        assert any("Dienstwagen" in c.label for c in est.annual_costs)

    def test_carbon_tax_info(self):
        est = calculate_germany_taxes(powertrain="ICE")
        assert any("碳税" in c.label for c in est.annual_costs)


class TestNetherlands:
    def test_bpm_for_ice(self):
        est = calculate_netherlands_taxes(co2_gkm=120, powertrain="ICE")
        assert any("BPM" in c.label for c in est.one_time_costs)

    def test_bev_bpm_exempt(self):
        est = calculate_netherlands_taxes(powertrain="BEV")
        assert any("BEV" in e for e in est.exemptions)

    def test_sepp_subsidy(self):
        est = calculate_netherlands_taxes(powertrain="BEV", vehicle_price=40_000)
        assert any("SEPP" in s.label for s in est.subsidies)

    def test_sepp_price_cap(self):
        est = calculate_netherlands_taxes(powertrain="BEV", vehicle_price=50_000)
        assert not any("SEPP" in s.label for s in est.subsidies)


class TestUnifiedDispatcher:
    def test_sweden_by_name(self):
        est = calculate_country_taxes("Sweden", co2_gkm=120, powertrain="ICE")
        assert est is not None
        assert est.country == "Sweden"

    def test_norway_by_code(self):
        est = calculate_country_taxes("NO", powertrain="BEV", vehicle_price=400_000)
        assert est is not None
        assert est.country == "Norway"

    def test_unknown_country(self):
        est = calculate_country_taxes("China")
        assert est is None

    def test_supported_countries(self):
        codes = supported_countries()
        codes_upper = {c.upper() for c in codes}
        assert "SE" in codes_upper
        assert "NO" in codes_upper
        assert "DE" in codes_upper
        assert len(codes) >= 10


class TestHungary:
    def test_bev_subsidy(self):
        est = calculate_country_taxes("HU", powertrain="BEV", vehicle_price=10_000_000)
        assert any("补贴" in s.label for s in est.subsidies)

    def test_bev_exempt_registration(self):
        est = calculate_country_taxes("HU", powertrain="BEV")
        assert any("exempt" in e.lower() for e in est.exemptions)


class TestCroatia:
    def test_bev_subsidy(self):
        est = calculate_country_taxes("HR", powertrain="BEV", vehicle_price=40_000)
        assert any("FZOEU" in s.label for s in est.subsidies)

    def test_bev_exempt(self):
        est = calculate_country_taxes("HR", powertrain="BEV")
        assert any("exempt" in e.lower() for e in est.exemptions)


class TestAustria:
    def test_nova_for_high_co2(self):
        est = calculate_country_taxes("AT", co2_gkm=180, powertrain="ICE")
        assert any("NoVA" in c.label for c in est.one_time_costs)

    def test_bev_subsidy(self):
        est = calculate_country_taxes("AT", powertrain="BEV", vehicle_price=50_000)
        assert any("Mobilität" in s.label for s in est.subsidies)

    def test_sachbezug_exempt_bev(self):
        est = calculate_country_taxes("AT", powertrain="BEV", vehicle_price=50_000, is_company_car=True)
        assert any("Sachbezug" in e for e in est.exemptions)


class TestCzechRepublic:
    def test_co2_fee_bands(self):
        est_low = calculate_country_taxes("CZ", co2_gkm=80, powertrain="ICE")
        est_high = calculate_country_taxes("CZ", co2_gkm=160, powertrain="ICE")
        fee_low = next(c.amount for c in est_low.one_time_costs if "登记费" in c.label)
        fee_high = next(c.amount for c in est_high.one_time_costs if "登记费" in c.label)
        assert fee_low < fee_high

    def test_bev_subsidy(self):
        est = calculate_country_taxes("CZ", powertrain="BEV", vehicle_price=1_000_000)
        assert any("补贴" in s.label for s in est.subsidies)

    def test_bev_exempt_annual(self):
        est = calculate_country_taxes("CZ", powertrain="BEV")
        assert any("exempt" in e.lower() for e in est.exemptions)
