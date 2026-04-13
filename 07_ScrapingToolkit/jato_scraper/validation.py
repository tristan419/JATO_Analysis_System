"""Validation rules for scraped MSRP observations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from jato_scraper.base import RawObservation

COUNTRY_CURRENCY: dict[str, str] = {
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


def _price_bounds(country: str) -> tuple[float, float]:
    cur = COUNTRY_CURRENCY.get(country)
    if cur and cur in _HIGH_DENOMINATION:
        return _HIGH_DENOMINATION[cur]
    return (MIN_PRICE, MAX_PRICE)


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
) -> ValidationResult:
    lo, hi = _price_bounds(country)
    if obs.msrp_value < lo:
        return ValidationResult(
            False,
            "price_range",
            f"msrp_value={obs.msrp_value} < {lo}",
        )
    if obs.msrp_value > hi:
        return ValidationResult(
            False,
            "price_range",
            f"msrp_value={obs.msrp_value} > {hi}",
        )
    return ValidationResult(True, "price_range", "ok")


def _rule_currency_match(
    obs: RawObservation,
    country: str,
) -> ValidationResult:
    expected = COUNTRY_CURRENCY.get(country)
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
) -> ValidationResult:
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
) -> BatchValidationReport:
    valid: list[RawObservation] = []
    rejected: list[tuple[RawObservation, list[ValidationResult]]] = []
    for obs in observations:
        failures: list[ValidationResult] = []
        for result in (
            _rule_positive_price(obs),
            _rule_price_range(obs, country),
            _rule_currency_match(obs, country),
            _rule_non_empty_model(obs),
            _rule_non_empty_trim(obs),
            _rule_delta_check(obs, previous_prices),
        ):
            if not result.ok:
                failures.append(result)
        if failures:
            rejected.append((obs, failures))
        else:
            valid.append(obs)
    return BatchValidationReport(valid=valid, rejected=rejected)
