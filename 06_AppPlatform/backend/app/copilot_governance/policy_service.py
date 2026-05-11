"""Policy/Tax service — loads and queries structured policy data."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from pydantic import BaseModel, Field

_POLICY_DIR = Path(__file__).resolve().parent / "catalog" / "datasets" / "policy_tax"

_CACHE: dict[str, dict[str, Any]] = {}


class PolicyRule(BaseModel):
    id: str
    name: str
    type: str
    effective_date: str = ""
    jurisdiction: str = ""
    description: str = ""
    formula: str = ""
    thresholds: list[dict[str, Any]] = Field(default_factory=list)
    unit: str = ""
    applies_to: list[str] = Field(default_factory=list)
    exempt: list[str] = Field(default_factory=list)
    source_url: str = ""
    confidence: str = "medium"
    status: str = "active"
    notes: str = ""


def _load_country_policy(country: str) -> dict[str, Any]:
    if country in _CACHE:
        return _CACHE[country]
    normalized = country.strip().lower()
    for yaml_file in _POLICY_DIR.glob("*.yaml"):
        try:
            with open(yaml_file) as fh:
                data = yaml.safe_load(fh)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        file_country = str(data.get("country", "")).strip().lower()
        file_code = str(data.get("country_code", "")).strip().lower()
        if normalized in (file_country, file_code):
            _CACHE[country] = data
            return data
    _CACHE[country] = {}
    return {}


def load_policy_rules(country: str) -> list[PolicyRule]:
    data = _load_country_policy(country)
    rules_data = data.get("rules", [])
    if not isinstance(rules_data, list):
        return []
    rules: list[PolicyRule] = []
    for item in rules_data:
        if not isinstance(item, dict):
            continue
        try:
            rules.append(PolicyRule(**item))
        except Exception:
            continue
    return rules


def find_rules_by_type(country: str, rule_type: str) -> list[PolicyRule]:
    return [
        rule for rule in load_policy_rules(country)
        if rule.type == rule_type
    ]


def search_policy(query: str, country: str) -> list[PolicyRule]:
    lowered = (query or "").lower()
    rules = load_policy_rules(country)
    if not rules:
        return []
    results: list[tuple[PolicyRule, int]] = []
    for rule in rules:
        score = 0
        searchable = f"{rule.name} {rule.description} {rule.type} {' '.join(rule.applies_to)}".lower()
        for word in lowered.split():
            if word in searchable:
                score += 1
        if score > 0:
            results.append((rule, score))
    results.sort(key=lambda x: x[1], reverse=True)
    return [rule for rule, _ in results]


def calculate_tax_estimate(
    country: str,
    co2_gkm: float = 0,
    vehicle_price: float = 0,
    weight_kg: float = 0,
    powertrain: str = "ICE",
    is_company_car: bool = False,
) -> dict[str, Any]:
    rules = load_policy_rules(country)
    estimates: list[dict[str, Any]] = []
    total_tax_estimate = 0.0

    for rule in rules:
        if rule.type == "co2_tax" and rule.thresholds:
            t = rule.thresholds[0]
            excess = co2_gkm - t.get("co2_gkm", 999)
            if excess > 0:
                rate = t.get("rate_per_g_sek", t.get("rate_eur_per_g", 0))
                base = t.get("base_sek", t.get("base", 0))
                tax = base + excess * rate
                estimates.append({
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "estimated": round(tax, 0),
                    "unit": rule.unit,
                    "confidence": rule.confidence,
                })

        elif rule.type == "purchase_tax" and vehicle_price > 0:
            for t in rule.thresholds:
                threshold = t.get("value_dkk", t.get("list_price_eur", 0))
                rate = t.get("rate_pct", 0)
                if vehicle_price <= (threshold or float("inf")):
                    tax = vehicle_price * rate / 100
                    estimates.append({
                        "rule_id": rule.id,
                        "rule_name": rule.name,
                        "estimated": round(tax, 0),
                        "rate_pct": rate,
                        "unit": rule.unit or "local currency",
                        "confidence": rule.confidence,
                    })
                    break

        elif rule.type == "subsidy" and rule.status != "expired":
            subsidy = rule.subsidy_eur if hasattr(rule, "subsidy_eur") else 0
            if subsidy > 0:
                estimates.append({
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "estimated": subsidy,
                    "unit": rule.unit or "EUR",
                    "confidence": rule.confidence,
                    "note": "subsidy; negative cost",
                })

    return {
        "country": country,
        "co2_gkm": co2_gkm,
        "powertrain": powertrain,
        "estimates": estimates,
        "total_annual_estimate": round(total_tax_estimate, 0),
        "disclaimer": "Estimates are indicative. Consult official sources for exact calculation.",
    }


def available_countries() -> list[str]:
    countries: list[str] = []
    for yaml_file in sorted(_POLICY_DIR.glob("*.yaml")):
        try:
            with open(yaml_file) as fh:
                data = yaml.safe_load(fh)
            if isinstance(data, dict) and data.get("country"):
                countries.append(data["country"])
        except Exception:
            continue
    return countries
