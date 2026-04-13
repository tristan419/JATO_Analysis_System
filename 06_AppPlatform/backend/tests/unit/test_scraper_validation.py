"""Tests for the MSRP scraper validation layer."""

from app.scraper.base import RawObservation
from app.scraper.validation import (
    validate_observations,
    BatchValidationReport,
    _rule_positive_price,
    _rule_price_range,
    _rule_currency_match,
    _rule_non_empty_model,
    _rule_non_empty_trim,
    _rule_delta_check,
    MIN_PRICE,
    MAX_PRICE,
    DELTA_THRESHOLD,
)


def _make_obs(**overrides) -> RawObservation:
    """Helper — build a valid RawObservation with optional overrides."""
    defaults = dict(
        official_model="3 Series",
        official_trim="320i Sedan",
        msrp_value=42_900.0,
        currency="EUR",
        tax_included=True,
        price_label="UVP",
        source_url="https://www.bmw.de",
    )
    defaults.update(overrides)
    return RawObservation(**defaults)


# ── individual rule tests ────────────────────────────────────────────

class TestPositivePrice:
    def test_positive(self):
        assert _rule_positive_price(_make_obs(msrp_value=100)).ok

    def test_zero(self):
        assert not _rule_positive_price(_make_obs(msrp_value=0)).ok

    def test_negative(self):
        assert not _rule_positive_price(_make_obs(msrp_value=-1)).ok


class TestPriceRange:
    def test_within_range(self):
        assert _rule_price_range(_make_obs(msrp_value=30_000)).ok

    def test_below_min(self):
        r = _rule_price_range(_make_obs(msrp_value=100))
        assert not r.ok
        assert str(MIN_PRICE) in r.reason

    def test_above_max(self):
        r = _rule_price_range(_make_obs(msrp_value=999_999))
        assert not r.ok
        assert str(MAX_PRICE) in r.reason


class TestCurrencyMatch:
    def test_correct_currency_germany(self):
        assert _rule_currency_match(_make_obs(currency="EUR"), "德国").ok

    def test_wrong_currency_germany(self):
        r = _rule_currency_match(_make_obs(currency="USD"), "德国")
        assert not r.ok
        assert "EUR" in r.reason

    def test_correct_currency_sweden(self):
        assert _rule_currency_match(_make_obs(currency="SEK"), "瑞典").ok

    def test_unknown_country_is_ok(self):
        assert _rule_currency_match(_make_obs(currency="ZZZ"), "未知国家").ok


class TestNonEmptyModel:
    def test_valid(self):
        assert _rule_non_empty_model(_make_obs(official_model="X5")).ok

    def test_empty(self):
        assert not _rule_non_empty_model(_make_obs(official_model="")).ok

    def test_whitespace(self):
        assert not _rule_non_empty_model(_make_obs(official_model="  ")).ok


class TestNonEmptyTrim:
    def test_valid(self):
        assert _rule_non_empty_trim(_make_obs(official_trim="xDrive")).ok

    def test_empty(self):
        assert not _rule_non_empty_trim(_make_obs(official_trim="")).ok


class TestDeltaCheck:
    def test_no_previous_prices(self):
        assert _rule_delta_check(_make_obs(), None).ok

    def test_new_model(self):
        prev = {("X5", "xDrive"): 80_000.0}
        assert _rule_delta_check(_make_obs(), prev).ok

    def test_within_threshold(self):
        prev = {("3 Series", "320i Sedan"): 40_000.0}
        obs = _make_obs(msrp_value=42_000.0)  # +5%
        assert _rule_delta_check(obs, prev).ok

    def test_exceeds_threshold(self):
        prev = {("3 Series", "320i Sedan"): 30_000.0}
        obs = _make_obs(msrp_value=42_900.0)  # +43%
        r = _rule_delta_check(obs, prev)
        assert not r.ok
        assert "delta" in r.reason.lower()


# ── batch validation tests ───────────────────────────────────────────

class TestValidateObservations:
    def test_all_valid(self):
        obs_list = [_make_obs(msrp_value=v) for v in (30_000, 45_000, 60_000)]
        report = validate_observations(obs_list, country="德国")
        assert len(report.valid) == 3
        assert len(report.rejected) == 0

    def test_mixed(self):
        obs_list = [
            _make_obs(msrp_value=30_000),
            _make_obs(msrp_value=-1),          # rejected: negative
            _make_obs(msrp_value=999_999),      # rejected: range
        ]
        report = validate_observations(obs_list, country="德国")
        assert len(report.valid) == 1
        assert len(report.rejected) == 2

    def test_total_count(self):
        obs_list = [_make_obs(), _make_obs(msrp_value=0)]
        report = validate_observations(obs_list, country="德国")
        assert report.total == 2

    def test_currency_rejected(self):
        obs_list = [_make_obs(currency="USD")]
        report = validate_observations(obs_list, country="德国")
        assert len(report.rejected) == 1
        failures = report.rejected[0][1]
        assert any(f.rule == "currency_match" for f in failures)

    def test_empty_model_rejected(self):
        obs_list = [_make_obs(official_model="")]
        report = validate_observations(obs_list, country="德国")
        assert len(report.rejected) == 1

    def test_with_previous_prices_delta(self):
        prev = {("3 Series", "320i Sedan"): 30_000.0}
        obs_list = [_make_obs(msrp_value=42_900)]  # +43% → rejected
        report = validate_observations(obs_list, country="德国", previous_prices=prev)
        assert len(report.rejected) == 1
        failures = report.rejected[0][1]
        assert any(f.rule == "delta_check" for f in failures)
