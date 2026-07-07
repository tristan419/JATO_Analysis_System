from jato_scraper.base import RawObservation
from jato_scraper.config_loader import _build_extractor_config
from jato_scraper.validation import validate_observations


def _eur_romania_observation() -> RawObservation:
    return RawObservation(
        official_model="ARKANA",
        official_trim="techno",
        msrp_value=29100.0,
        currency="EUR",
        tax_included=True,
        price_label="MSRP",
        source_url="https://www.renault.ro/vehicule-hibride/arkana-e-tech.html",
    )


def test_source_expected_currency_allows_official_eur_in_romania() -> None:
    report = validate_observations(
        [_eur_romania_observation()],
        country="罗马尼亚",
        expected_currency="EUR",
        source_price_semantics="base_msrp",
    )

    assert len(report.valid) == 1
    assert report.rejected == []


def test_country_default_currency_still_rejects_without_source_override() -> None:
    report = validate_observations(
        [_eur_romania_observation()],
        country="罗马尼亚",
        source_price_semantics="base_msrp",
    )

    assert report.valid == []
    rejected_obs, failures = report.rejected[0]
    assert rejected_obs.currency == "EUR"
    assert [failure.rule for failure in failures] == ["currency_match"]
    assert "expected RON" in failures[0].reason


def test_invalid_expected_currency_falls_back_to_country_default() -> None:
    report = validate_observations(
        [_eur_romania_observation()],
        country="ro",
        expected_currency="TODO",
        source_price_semantics="base_msrp",
    )

    assert report.valid == []
    assert report.rejected[0][1][0].rule == "currency_match"


def test_extractor_config_uses_profile_default_currency_as_expected_currency() -> None:
    config = _build_extractor_config({
        "source_code": "renault_arkana_ro_draft_scrapling",
        "country": "罗马尼亚",
        "brand": "RENAULT",
        "source_url": "https://www.renault.ro/vehicule-hibride/arkana-e-tech.html",
        "profile": {"default_currency": "EUR"},
    })

    assert config.expected_currency == "EUR"
