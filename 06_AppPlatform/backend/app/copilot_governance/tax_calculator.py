"""Per-country tax calculators — CO2 tax, weight tax, purchase tax, subsidies."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CostItem(BaseModel):
    label: str
    amount: float
    currency: str = ""
    period: str = ""
    category: str = ""
    note: str = ""


class TaxEstimate(BaseModel):
    country: str
    country_code: str = ""
    currency: str = ""
    co2_gkm: float = 0.0
    weight_kg: float = 0.0
    vehicle_price: float = 0.0
    powertrain: str = "ICE"
    is_company_car: bool = False
    one_time_costs: list[CostItem] = Field(default_factory=list)
    annual_costs: list[CostItem] = Field(default_factory=list)
    total_one_time: float = 0.0
    total_annual: float = 0.0
    total_3year: float = 0.0
    exemptions: list[str] = Field(default_factory=list)
    subsidies: list[CostItem] = Field(default_factory=list)
    disclaimer: str = "Estimates are indicative. Consult official sources for exact calculation."


def _is_exempt(powertrain: str, exempt_list: list[str]) -> bool:
    return powertrain.upper() in {e.upper() for e in exempt_list}


# ═══════════════════════════════════════════════════════════════
# Sweden
# ═══════════════════════════════════════════════════════════════

def calculate_sweden_taxes(
    co2_gkm: float = 0,
    weight_kg: float = 0,
    vehicle_price: float = 0,
    powertrain: str = "ICE",
    is_company_car: bool = False,
) -> TaxEstimate:
    est = TaxEstimate(
        country="Sweden", country_code="SE", currency="SEK",
        co2_gkm=co2_gkm, weight_kg=weight_kg, vehicle_price=vehicle_price,
        powertrain=powertrain, is_company_car=is_company_car,
    )
    be_bev = powertrain.upper() == "BEV"

    # Malus CO2 tax (annual, first 3 years)
    if not be_bev and co2_gkm > 70:
        malus = 132 * (co2_gkm - 70)
        if malus < 360:
            malus = 360
        est.annual_costs.append(CostItem(
            label="Malus CO₂税 (前3年)", amount=round(malus), currency="SEK",
            period="annual", category="co2_tax",
            note=f"132 SEK × ({co2_gkm:.0f} - 70) g/km",
        ))
    elif not be_bev and co2_gkm <= 70:
        est.annual_costs.append(CostItem(
            label="基础车辆税", amount=360, currency="SEK",
            period="annual", category="base_tax",
            note="CO₂ ≤70 g/km, minimum rate",
        ))
    elif be_bev:
        est.exemptions.append("BEV exempt from Malus CO₂ tax")
        est.annual_costs.append(CostItem(
            label="BEV基础税", amount=360, currency="SEK",
            period="annual", category="base_tax", note="BEV minimum rate",
        ))

    # Company car benefit (förmånsbil)
    if is_company_car and vehicle_price > 0:
        benefit_base = vehicle_price * 0.09
        if be_bev:
            benefit_base *= 0.75
        est.annual_costs.append(CostItem(
            label="公司车福利税 (förmån)", amount=round(benefit_base), currency="SEK",
            period="annual", category="benefit_in_kind",
            note=f"9% of MSRP{' × 0.75 BEV' if be_bev else ''}",
        ))

    est.total_annual = sum(c.amount for c in est.annual_costs)
    est.total_3year = est.total_annual * 3
    est.total_one_time = sum(c.amount for c in est.one_time_costs)
    return est


# ═══════════════════════════════════════════════════════════════
# Norway
# ═══════════════════════════════════════════════════════════════

def calculate_norway_taxes(
    co2_gkm: float = 0,
    weight_kg: float = 0,
    vehicle_price: float = 0,
    powertrain: str = "ICE",
    is_company_car: bool = False,
) -> TaxEstimate:
    est = TaxEstimate(
        country="Norway", country_code="NO", currency="NOK",
        co2_gkm=co2_gkm, weight_kg=weight_kg, vehicle_price=vehicle_price,
        powertrain=powertrain, is_company_car=is_company_car,
    )
    be_bev = powertrain.upper() == "BEV"

    # VAT exemption for BEV
    if be_bev and vehicle_price > 0:
        exempt_portion = min(vehicle_price, 500_000)
        vat_saved = exempt_portion * 0.25
        est.one_time_costs.append(CostItem(
            label="BEV增值税豁免", amount=round(vat_saved), currency="NOK",
            period="one_time", category="tax_exemption",
            note=f"25% VAT exempt on first 500,000 NOK ({exempt_portion:,.0f} × 0.25)",
        ))
        est.exemptions.append(f"BEV: 25% VAT exempt up to 500,000 NOK (saves ~{vat_saved:,.0f} NOK)")
        est.subsidies.append(CostItem(
            label="VAT豁免", amount=round(vat_saved), currency="NOK",
            period="one_time", category="vat_exemption",
        ))
    elif not be_bev and vehicle_price > 0:
        est.one_time_costs.append(CostItem(
            label="25%增值税", amount=round(vehicle_price * 0.25), currency="NOK",
            period="one_time", category="vat",
        ))

    # Weight tax (annual)
    if weight_kg > 500:
        weight_tax = 12.50 * (weight_kg - 500) * 365 / 1000
        est.annual_costs.append(CostItem(
            label="重量税 (Trafikkforsikringsavgift)",
            amount=round(weight_tax), currency="NOK",
            period="annual", category="weight_tax",
            note=f"12.50 NOK/day/kg above 500 kg ({weight_kg:.0f} - 500 = {weight_kg - 500:.0f} kg)",
        ))

    # Toll road discount (BEV)
    if be_bev:
        est.subsidies.append(CostItem(
            label="BEV过路费折扣", amount=0, currency="NOK",
            period="annual", category="toll_discount",
            note="70% of full toll rate (reduced from 100%)",
        ))

    est.total_one_time = sum(c.amount for c in est.one_time_costs)
    est.total_annual = sum(c.amount for c in est.annual_costs)
    est.total_3year = est.total_annual * 3
    return est


# ═══════════════════════════════════════════════════════════════
# Finland
# ═══════════════════════════════════════════════════════════════

def calculate_finland_taxes(
    co2_gkm: float = 0,
    weight_kg: float = 0,
    vehicle_price: float = 0,
    powertrain: str = "ICE",
    is_company_car: bool = False,
) -> TaxEstimate:
    est = TaxEstimate(
        country="Finland", country_code="FI", currency="EUR",
        co2_gkm=co2_gkm, weight_kg=weight_kg, vehicle_price=vehicle_price,
        powertrain=powertrain, is_company_car=is_company_car,
    )
    be_bev = powertrain.upper() == "BEV"

    # Car tax (Autovero) — one-time at registration
    if co2_gkm <= 0:
        rate = 2.7
    elif co2_gkm <= 100:
        rate = 2.7 + (co2_gkm / 100) * (15 - 2.7)
    elif co2_gkm <= 200:
        rate = 15 + ((co2_gkm - 100) / 100) * (30 - 15)
    elif co2_gkm <= 360:
        rate = 30 + ((co2_gkm - 200) / 160) * (50 - 30)
    else:
        rate = 50
    car_tax = vehicle_price * rate / 100
    est.one_time_costs.append(CostItem(
        label="购置税 (Autovero)", amount=round(car_tax), currency="EUR",
        period="one_time", category="purchase_tax",
        note=f"CO₂ {co2_gkm:.0f} g/km → {rate:.1f}% of {vehicle_price:,.0f} EUR",
    ))

    # BEV subsidy
    if be_bev and vehicle_price <= 50_000:
        est.subsidies.append(CostItem(
            label="BEV购置补贴", amount=2000, currency="EUR",
            period="one_time", category="subsidy",
            note="Max 2,000 EUR, price cap 50,000 EUR",
        ))
        est.one_time_costs.append(CostItem(
            label="BEV补贴", amount=-2000, currency="EUR",
            period="one_time", category="subsidy",
        ))

    # Company car CO2 benefit
    if is_company_car and be_bev:
        est.annual_costs.append(CostItem(
            label="公司车BEV减免", amount=-2400, currency="EUR",
            period="annual", category="benefit_in_kind",
            note="200 EUR/month reduction (temporary)",
        ))

    est.total_one_time = sum(c.amount for c in est.one_time_costs)
    est.total_annual = sum(c.amount for c in est.annual_costs)
    est.total_3year = est.total_one_time + est.total_annual * 3
    return est


# ═══════════════════════════════════════════════════════════════
# Denmark
# ═══════════════════════════════════════════════════════════════

def calculate_denmark_taxes(
    co2_gkm: float = 0,
    weight_kg: float = 0,
    vehicle_price: float = 0,
    powertrain: str = "ICE",
    is_company_car: bool = False,
) -> TaxEstimate:
    est = TaxEstimate(
        country="Denmark", country_code="DK", currency="DKK",
        co2_gkm=co2_gkm, weight_kg=weight_kg, vehicle_price=vehicle_price,
        powertrain=powertrain, is_company_car=is_company_car,
    )
    be_bev = powertrain.upper() == "BEV"

    taxable = vehicle_price
    be_deduction = 157_500 if be_bev else 0
    taxable = max(0, taxable - be_deduction)

    if taxable <= 73_000:
        reg_tax = taxable * 0.25
        note = f"25% band (≤73k DKK)"
    elif taxable <= 218_000:
        reg_tax = 73_000 * 0.25 + (taxable - 73_000) * 0.85
        note = f"25%/85% bands"
    else:
        reg_tax = 73_000 * 0.25 + (218_000 - 73_000) * 0.85 + (taxable - 218_000) * 1.50
        note = f"25%/85%/150% bands"

    if be_bev:
        reg_tax *= 0.60
        note += f", BEV 60% rate"
        est.exemptions.append(f"BEV: {be_deduction:,.0f} DKK deduction, 60% rate")

    est.one_time_costs.append(CostItem(
        label="登记税 (Registreringsafgift)", amount=round(reg_tax), currency="DKK",
        period="one_time", category="purchase_tax", note=note,
    ))

    # Green ownership tax (semi-annual)
    if be_bev:
        green_tax = 780 * 2
    elif co2_gkm <= 50:
        green_tax = 1_500 * 2
    elif co2_gkm <= 100:
        green_tax = 3_000 * 2
    else:
        green_tax = 6_000 * 2
    est.annual_costs.append(CostItem(
        label="绿色所有权税 (Grøn ejerafgift)", amount=round(green_tax), currency="DKK",
        period="annual", category="annual_tax",
        note=f"{'BEV minimum' if be_bev else 'CO₂-based'} rate",
    ))

    est.total_one_time = sum(c.amount for c in est.one_time_costs)
    est.total_annual = sum(c.amount for c in est.annual_costs)
    est.total_3year = est.total_one_time + est.total_annual * 3
    return est


# ═══════════════════════════════════════════════════════════════
# Germany
# ═══════════════════════════════════════════════════════════════

def calculate_germany_taxes(
    co2_gkm: float = 0,
    weight_kg: float = 0,
    vehicle_price: float = 0,
    powertrain: str = "ICE",
    is_company_car: bool = False,
) -> TaxEstimate:
    est = TaxEstimate(
        country="Germany", country_code="DE", currency="EUR",
        co2_gkm=co2_gkm, weight_kg=weight_kg, vehicle_price=vehicle_price,
        powertrain=powertrain, is_company_car=is_company_car,
    )
    be_bev = powertrain.upper() == "BEV"

    # Annual CO2 vehicle tax
    if be_bev:
        est.annual_costs.append(CostItem(
            label="BEV免税 (10年)", amount=0, currency="EUR",
            period="annual", category="annual_tax",
            note="BEV exempt for 10 years from registration",
        ))
        est.exemptions.append("BEV: 10-year vehicle tax exemption")
    elif co2_gkm > 95:
        surcharge = 0.0
        remaining = co2_gkm - 95
        brackets = [(20, 2.0), (20, 2.2), (20, 2.5), (999, 3.0)]
        for width, rate in brackets:
            chunk = min(remaining, width)
            surcharge += chunk * rate
            remaining -= chunk
            if remaining <= 0:
                break
        est.annual_costs.append(CostItem(
            label="CO₂附加税", amount=round(surcharge), currency="EUR",
            period="annual", category="co2_tax",
            note=f"CO₂ {co2_gkm:.0f} g/km, graduated surcharge",
        ))
    else:
        est.annual_costs.append(CostItem(
            label="基础车辆税 (CO₂ ≤95)", amount=20, currency="EUR",
            period="annual", category="base_tax",
        ))

    # Company car BEV benefit
    if is_company_car and be_bev and vehicle_price > 0:
        rate = 0.0025 if vehicle_price <= 70_000 else 0.005
        monthly = vehicle_price * rate
        est.annual_costs.append(CostItem(
            label="公司车BEV福利税 (Dienstwagen)",
            amount=round(monthly * 12), currency="EUR",
            period="annual", category="benefit_in_kind",
            note=f"{rate*100:.2f}%/month of {vehicle_price:,.0f} EUR",
        ))

    # Carbon tax (fuel) — informational
    est.annual_costs.append(CostItem(
        label="碳税 (燃料, 估算)", amount=round(55 * 2.5), currency="EUR",
        period="annual", category="carbon_tax",
        note="55 EUR/t CO₂ (2025), ~2.5 t/year avg car",
    ))

    est.total_annual = sum(c.amount for c in est.annual_costs)
    est.total_3year = est.total_annual * 3
    est.total_one_time = sum(c.amount for c in est.one_time_costs)
    return est


# ═══════════════════════════════════════════════════════════════
# Netherlands
# ═══════════════════════════════════════════════════════════════

def calculate_netherlands_taxes(
    co2_gkm: float = 0,
    weight_kg: float = 0,
    vehicle_price: float = 0,
    powertrain: str = "ICE",
    is_company_car: bool = False,
) -> TaxEstimate:
    est = TaxEstimate(
        country="Netherlands", country_code="NL", currency="EUR",
        co2_gkm=co2_gkm, weight_kg=weight_kg, vehicle_price=vehicle_price,
        powertrain=powertrain, is_company_car=is_company_car,
    )
    be_bev = powertrain.upper() == "BEV"

    # BPM registration tax (one-time, CO2-based)
    if not be_bev and co2_gkm > 0:
        bpm = max(0, (co2_gkm - 50) * 200)
        est.one_time_costs.append(CostItem(
            label="BPM登记税 (CO₂)", amount=round(bpm), currency="EUR",
            period="one_time", category="purchase_tax",
            note=f"~200 EUR/g above 50 g/km: ({co2_gkm:.0f} - 50) × 200",
        ))
    elif be_bev:
        est.exemptions.append("BEV: BPM exempt (0 EUR)")
        est.one_time_costs.append(CostItem(
            label="BPM (BEV豁免)", amount=0, currency="EUR",
            period="one_time", category="tax_exemption",
        ))

    # MRB motor vehicle tax (quarterly, weight-based)
    if be_bev:
        mrb = weight_kg * 0.25 * 4
        est.annual_costs.append(CostItem(
            label="MRB道路税 (BEV 25%)", amount=round(mrb), currency="EUR",
            period="annual", category="annual_tax",
            note="BEV: 25% of full rate until 2025",
        ))
    else:
        mrb = weight_kg * 1.0 * 4
        est.annual_costs.append(CostItem(
            label="MRB道路税", amount=round(mrb), currency="EUR",
            period="annual", category="annual_tax",
            note="Weight-based quarterly tax",
        ))

    # Bijtelling (company car benefit)
    if is_company_car and be_bev and vehicle_price > 0:
        below = min(vehicle_price, 30_000)
        above = max(0, vehicle_price - 30_000)
        bijtelling = below * 0.16 + above * 0.22
        est.annual_costs.append(CostItem(
            label="公司车福利税 (Bijtelling BEV)",
            amount=round(bijtelling), currency="EUR",
            period="annual", category="benefit_in_kind",
            note="16% up to 30k, 22% above",
        ))
    elif is_company_car and vehicle_price > 0:
        est.annual_costs.append(CostItem(
            label="公司车福利税 (Bijtelling ICE)", amount=round(vehicle_price * 0.22),
            currency="EUR", period="annual", category="benefit_in_kind",
        ))

    # SEPP subsidy for BEV
    if be_bev and vehicle_price <= 45_000:
        est.subsidies.append(CostItem(
            label="SEPP BEV补贴", amount=2_950, currency="EUR",
            period="one_time", category="subsidy",
        ))
        est.one_time_costs.append(CostItem(
            label="SEPP补贴", amount=-2_950, currency="EUR",
            period="one_time", category="subsidy",
        ))

    est.total_one_time = sum(c.amount for c in est.one_time_costs)
    est.total_annual = sum(c.amount for c in est.annual_costs)
    est.total_3year = est.total_one_time + est.total_annual * 3
    return est


# ═══════════════════════════════════════════════════════════════
# Unified dispatcher
# ═══════════════════════════════════════════════════════════════

_CALCULATORS: dict[str, object] = {
    "sweden": calculate_sweden_taxes,
    "se": calculate_sweden_taxes,
    "norway": calculate_norway_taxes,
    "no": calculate_norway_taxes,
    "finland": calculate_finland_taxes,
    "fi": calculate_finland_taxes,
    "denmark": calculate_denmark_taxes,
    "dk": calculate_denmark_taxes,
    "germany": calculate_germany_taxes,
    "de": calculate_germany_taxes,
    "netherlands": calculate_netherlands_taxes,
    "nl": calculate_netherlands_taxes,
}


def calculate_country_taxes(
    country: str,
    *,
    co2_gkm: float = 0,
    weight_kg: float = 0,
    vehicle_price: float = 0,
    powertrain: str = "ICE",
    is_company_car: bool = False,
) -> TaxEstimate | None:
    key = country.strip().lower()
    calc = _CALCULATORS.get(key)
    if calc is None:
        return None
    return calc(
        co2_gkm=co2_gkm,
        weight_kg=weight_kg,
        vehicle_price=vehicle_price,
        powertrain=powertrain,
        is_company_car=is_company_car,
    )


def supported_countries() -> list[str]:
    return sorted({k for k in _CALCULATORS if len(k) == 2})
