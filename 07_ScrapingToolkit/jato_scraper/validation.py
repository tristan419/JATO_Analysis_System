"""Validation rules for scraped MSRP observations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from jato_scraper.base import RawObservation

COUNTRY_CURRENCY: dict[str, str] = {
    "at": "EUR",
    "be": "EUR",
    "ch": "CHF",
    "cz": "CZK",
    "de": "EUR",
    "dk": "DKK",
    "es": "EUR",
    "fi": "EUR",
    "fr": "EUR",
    "gr": "EUR",
    "hr": "EUR",
    "hu": "HUF",
    "it": "EUR",
    "nl": "EUR",
    "no": "NOK",
    "pl": "PLN",
    "pt": "EUR",
    "ro": "RON",
    "se": "SEK",
    "si": "EUR",
    "sk": "EUR",
    "德国": "EUR",
    "法国": "EUR",
    "意大利": "EUR",
    "奥地利": "EUR",
    "比利时": "EUR",
    "荷兰": "EUR",
    "芬兰": "EUR",
    "希腊": "EUR",
    "葡萄牙": "EUR",
    "斯洛伐克": "EUR",
    "斯洛文尼亚": "EUR",
    "克罗地亚": "EUR",
    "瑞典": "SEK",
    "挪威": "NOK",
    "丹麦": "DKK",
    "瑞士": "CHF",
    "捷克": "CZK",
    "匈牙利": "HUF",
    "波兰": "PLN",
    "罗马尼亚": "RON",
}
MIN_PRICE = 5_000.0
MAX_PRICE = 500_000.0
DELTA_THRESHOLD = 0.30
MONTHLY_MIN_AMOUNT = 50.0
MONTHLY_MAX_AMOUNT = 20_000.0
ALLOWANCE_MIN_AMOUNT = 1.0
ALLOWANCE_MAX_AMOUNT = 100_000.0
MONTHLY_PRICE_SEMANTICS = frozenset({
    "lease_monthly",
    "finance_monthly",
    "subscription_monthly",
    "monthly_payment",
})
ALLOWANCE_PRICE_SEMANTICS = frozenset({
    "subsidy_amount",
    "government_subsidy",
    "rebate_amount",
    "cash_bonus",
})
CURRENT_PRICE_SEMANTICS = frozenset({
    "base_msrp",
    "cash_msrp",
    "list_price",
})

# Currencies where absolute price values are much larger
_HIGH_DENOMINATION: dict[str, tuple[float, float]] = {
    "SEK": (50_000.0, 5_000_000.0),
    "NOK": (50_000.0, 5_000_000.0),
    "DKK": (50_000.0, 5_000_000.0),
    "CHF": (10_000.0, 1_500_000.0),
    "PLN": (20_000.0, 2_000_000.0),
    "RON": (20_000.0, 2_000_000.0),
    "HUF": (1_000_000.0, 50_000_000.0),
    "CZK": (200_000.0, 10_000_000.0),
}
_HIGH_DENOMINATION_MONTHLY: dict[str, tuple[float, float]] = {
    "SEK": (500.0, 200_000.0),
    "NOK": (500.0, 200_000.0),
    "DKK": (500.0, 200_000.0),
    "CHF": (50.0, 30_000.0),
    "PLN": (200.0, 200_000.0),
    "RON": (200.0, 200_000.0),
    "HUF": (10_000.0, 5_000_000.0),
    "CZK": (1_000.0, 500_000.0),
}
_HIGH_DENOMINATION_ALLOWANCE: dict[str, tuple[float, float]] = {
    "SEK": (1.0, 1_000_000.0),
    "NOK": (1.0, 1_000_000.0),
    "DKK": (1.0, 1_000_000.0),
    "CHF": (1.0, 150_000.0),
    "PLN": (1.0, 500_000.0),
    "RON": (1.0, 500_000.0),
    "HUF": (1.0, 20_000_000.0),
    "CZK": (1.0, 3_000_000.0),
}


def _expected_currency(country: str) -> str | None:
    normalized = str(country or "").strip()
    return (
        COUNTRY_CURRENCY.get(normalized.lower())
        or COUNTRY_CURRENCY.get(normalized)
    )


def _price_bounds(country: str) -> tuple[float, float]:
    cur = _expected_currency(country)
    if cur and cur in _HIGH_DENOMINATION:
        return _HIGH_DENOMINATION[cur]
    return (MIN_PRICE, MAX_PRICE)


def _amount_bounds(
    country: str,
    *,
    semantics: str,
) -> tuple[float, float]:
    cur = _expected_currency(country)
    if semantics in MONTHLY_PRICE_SEMANTICS:
        if cur and cur in _HIGH_DENOMINATION_MONTHLY:
            return _HIGH_DENOMINATION_MONTHLY[cur]
        return (MONTHLY_MIN_AMOUNT, MONTHLY_MAX_AMOUNT)
    if semantics in ALLOWANCE_PRICE_SEMANTICS:
        if cur and cur in _HIGH_DENOMINATION_ALLOWANCE:
            return _HIGH_DENOMINATION_ALLOWANCE[cur]
        return (ALLOWANCE_MIN_AMOUNT, ALLOWANCE_MAX_AMOUNT)
    return _price_bounds(country)


def _price_semantics(
    obs: RawObservation,
    source_price_semantics: str | None = None,
) -> str:
    raw_payload = obs.raw_payload if isinstance(obs.raw_payload, dict) else {}
    pricing_context = raw_payload.get("pricingContext")
    if isinstance(pricing_context, dict):
        value = pricing_context.get("price_semantics")
        if value is not None and str(value).strip():
            return str(value).strip()
    value = raw_payload.get("price_semantics")
    if value is not None and str(value).strip():
        return str(value).strip()
    if source_price_semantics is not None and str(source_price_semantics).strip():
        return str(source_price_semantics).strip()
    return "base_msrp"


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    rule: str
    reason: str


@dataclass
class BatchValidationReport:
    valid: list[RawObservation]
    rejected: list[tuple[RawObservation, list[ValidationResult]]]

    @property
    def total(self) -> int:
        return len(self.valid) + len(self.rejected)


def _rule_positive_price(obs: RawObservation) -> ValidationResult:
    if obs.msrp_value <= 0:
        return ValidationResult(
            False,
            "positive_price",
            f"msrp_value={obs.msrp_value} <= 0",
        )
    return ValidationResult(True, "positive_price", "ok")


def _rule_price_range(
    obs: RawObservation,
    country: str = "",
    source_price_semantics: str | None = None,
) -> ValidationResult:
    semantics = _price_semantics(obs, source_price_semantics)
    lo, hi = _amount_bounds(country, semantics=semantics)
    if obs.msrp_value < lo:
        return ValidationResult(
            False,
            "price_range",
            f"msrp_value={obs.msrp_value} < {lo} for {semantics}",
        )
    if obs.msrp_value > hi:
        return ValidationResult(
            False,
            "price_range",
            f"msrp_value={obs.msrp_value} > {hi} for {semantics}",
        )
    return ValidationResult(True, "price_range", "ok")


def _rule_currency_match(
    obs: RawObservation,
    country: str,
) -> ValidationResult:
    expected = _expected_currency(country)
    if expected and obs.currency != expected:
        return ValidationResult(
            False,
            "currency_match",
            f"currency={obs.currency} but expected {expected} for {country}",
        )
    return ValidationResult(True, "currency_match", "ok")


def _rule_non_empty_model(obs: RawObservation) -> ValidationResult:
    if not obs.official_model or not obs.official_model.strip():
        return ValidationResult(
            False,
            "non_empty_model",
            "official_model is empty",
        )
    return ValidationResult(True, "non_empty_model", "ok")


def _rule_non_empty_trim(obs: RawObservation) -> ValidationResult:
    if not obs.official_trim or not obs.official_trim.strip():
        return ValidationResult(
            False,
            "non_empty_trim",
            "official_trim is empty",
        )
    return ValidationResult(True, "non_empty_trim", "ok")


def _rule_delta_check(
    obs: RawObservation,
    previous_prices: dict[tuple[str, str], float] | None,
    source_price_semantics: str | None = None,
) -> ValidationResult:
    semantics = _price_semantics(obs, source_price_semantics)
    if semantics not in CURRENT_PRICE_SEMANTICS:
        return ValidationResult(
            True,
            "delta_check",
            f"non-current price semantics {semantics} — skipped",
        )
    if previous_prices is None:
        return ValidationResult(
            True,
            "delta_check",
            "no previous prices — skipped",
        )
    key = (obs.official_model, obs.official_trim)
    prev = previous_prices.get(key)
    if prev is None:
        return ValidationResult(
            True,
            "delta_check",
            "new model/trim — skipped",
        )
    if prev == 0:
        return ValidationResult(
            False,
            "delta_check",
            "previous price was 0 — cannot compute delta",
        )
    pct = abs(obs.msrp_value - prev) / prev
    if pct > DELTA_THRESHOLD:
        return ValidationResult(
            False,
            "delta_check",
            f"price delta {pct:.0%} exceeds ±{DELTA_THRESHOLD:.0%} "
            f"(was {prev}, now {obs.msrp_value})",
        )
    return ValidationResult(True, "delta_check", "ok")


def validate_observations(
    observations: Sequence[RawObservation],
    country: str,
    previous_prices: dict[tuple[str, str], float] | None = None,
    source_price_semantics: str | None = None,
) -> BatchValidationReport:
    valid: list[RawObservation] = []
    rejected: list[tuple[RawObservation, list[ValidationResult]]] = []
    for obs in observations:
        failures: list[ValidationResult] = []
        for result in (
            _rule_positive_price(obs),
            _rule_price_range(obs, country, source_price_semantics),
            _rule_currency_match(obs, country),
            _rule_non_empty_model(obs),
            _rule_non_empty_trim(obs),
            _rule_delta_check(obs, previous_prices, source_price_semantics),
        ):
            if not result.ok:
                failures.append(result)
        if failures:
            rejected.append((obs, failures))
        else:
            valid.append(obs)
    return BatchValidationReport(valid=valid, rejected=rejected)
