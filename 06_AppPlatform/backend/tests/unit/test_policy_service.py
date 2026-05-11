"""Tests for policy/tax service."""

from __future__ import annotations

from app.copilot_governance.policy_service import (
    PolicyRule,
    available_countries,
    calculate_tax_estimate,
    find_rules_by_type,
    load_policy_rules,
    search_policy,
)


class TestLoadPolicyRules:
    def test_sweden_has_rules(self):
        rules = load_policy_rules("Sweden")
        assert len(rules) >= 2
        rule_ids = {r.id for r in rules}
        assert "se_malus_co2" in rule_ids

    def test_sweden_case_insensitive(self):
        rules = load_policy_rules("sweden")
        assert len(rules) >= 2

    def test_sweden_by_code(self):
        rules = load_policy_rules("SE")
        assert len(rules) >= 2

    def test_unknown_country_empty(self):
        rules = load_policy_rules("UnknownCountry")
        assert rules == []

    def test_all_rule_types_are_valid(self):
        for country in available_countries():
            rules = load_policy_rules(country)
            for rule in rules:
                assert isinstance(rule, PolicyRule)
                assert rule.id
                assert rule.name
                assert rule.type

    def test_find_by_type(self):
        rules = find_rules_by_type("Sweden", "co2_tax")
        assert len(rules) >= 1
        assert all(r.type == "co2_tax" for r in rules)

    def test_search_policy(self):
        results = search_policy("malus", "Sweden")
        assert len(results) >= 1
        assert any("malus" in r.name.lower() or "malus" in r.id.lower() for r in results)

    def test_available_countries(self):
        countries = available_countries()
        assert "Sweden" in countries
        assert "Norway" in countries
        assert len(countries) >= 6


class TestCalculateTaxEstimate:
    def test_sweden_co2_tax(self):
        result = calculate_tax_estimate(
            country="Sweden",
            co2_gkm=120,
            powertrain="ICE",
        )
        assert result["country"] == "Sweden"
        assert len(result["estimates"]) >= 1

    def test_bev_exempt(self):
        result = calculate_tax_estimate(
            country="Norway",
            co2_gkm=0,
            powertrain="BEV",
        )
        assert "disclaimer" in result
