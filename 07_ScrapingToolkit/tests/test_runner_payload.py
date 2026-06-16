from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation
from jato_scraper.runner import build_batch_payload
from jato_scraper.validation import BatchValidationReport


class DummyExtractor(BaseExtractor):
    def extract(self) -> list[RawObservation]:
        return []


def test_build_batch_payload_preserves_raw_payload_as_source_context() -> None:
    extractor = DummyExtractor(
        ExtractorConfig(
            source_code="volvo_se_xc60",
            country="瑞典",
            brand="Volvo",
            source_url="https://example.test/xc60",
        )
    )
    observation = RawObservation(
        official_model="XC60",
        official_trim="Ultra",
        msrp_value=773000,
        currency="SEK",
        tax_included=True,
        price_label="List price",
        source_url="https://example.test/xc60",
        raw_payload={
            "priceText": "773 000 kr",
            "monthly_payment": 5990,
            "term_months": 36,
            "finance_type": "private_lease",
            "finance_currency": "SEK",
            "price_semantics": "lease_monthly",
        },
    )

    payload = build_batch_payload(
        extractor,
        BatchValidationReport(valid=[observation], rejected=[]),
        source_id="source-1",
    )

    source_context = payload["observations"][0]["source_context_json"]
    assert source_context["rawPayload"]["priceText"] == "773 000 kr"
    assert source_context["pricingContext"] == {
        "monthly_payment": 5990,
        "term_months": 36,
        "finance_type": "private_lease",
        "finance_currency": "SEK",
        "price_semantics": "lease_monthly",
    }


def test_build_batch_payload_preserves_explicit_pricing_context() -> None:
    extractor = DummyExtractor(
        ExtractorConfig(
            source_code="volvo_se_xc60",
            country="瑞典",
            brand="Volvo",
            source_url="https://example.test/xc60",
        )
    )
    observation = RawObservation(
        official_model="XC60",
        official_trim="Ultra",
        msrp_value=773000,
        currency="SEK",
        tax_included=True,
        price_label="List price",
        source_url="https://example.test/xc60",
        raw_payload={
            "priceText": "773 000 kr",
            "pricingContext": {
                "monthly_payment": 5990,
                "price_semantics": "lease_monthly",
            },
        },
    )

    payload = build_batch_payload(
        extractor,
        BatchValidationReport(valid=[observation], rejected=[]),
        source_id="source-1",
    )

    source_context = payload["observations"][0]["source_context_json"]
    assert source_context["rawPayload"]["pricingContext"]["monthly_payment"] == 5990
    assert source_context["pricingContext"] == {
        "monthly_payment": 5990,
        "price_semantics": "lease_monthly",
    }
