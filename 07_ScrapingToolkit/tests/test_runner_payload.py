import requests

from jato_scraper import runner
from jato_scraper.base import BaseExtractor, ExtractorConfig, RawObservation
from jato_scraper.runner import build_batch_payload
from jato_scraper.validation import BatchValidationReport


class DummyExtractor(BaseExtractor):
    def extract(self) -> list[RawObservation]:
        return []


class FakeResponse:
    def __init__(
        self,
        payload: dict,
        status_code: int = 200,
    ) -> None:
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")

    def json(self) -> dict:
        return self.payload


def test_verify_write_auth_requires_editor_role(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_get(url: str, headers: dict[str, str], timeout: int):
        captured["url"] = url
        captured["headers"] = headers
        captured["timeout"] = timeout
        return FakeResponse({"role": "viewer", "username": "msrp-cron"})

    monkeypatch.setattr(runner.requests, "get", fake_get)

    result = runner._verify_write_auth(
        "https://example.test/v1",
        auth_token="token-1",
        user_name="msrp-cron",
    )

    assert result == {
        "ok": False,
        "status": "auth_failed",
        "reason": "write_role_required",
        "role": "viewer",
        "requiredRole": "editor",
    }
    assert captured["url"] == "https://example.test/v1/auth/me"
    assert captured["headers"] == {
        "X-Auth-Token": "token-1",
        "X-User-Name": "msrp-cron",
    }
    assert captured["timeout"] == 15


def test_verify_write_auth_accepts_editor_role(monkeypatch) -> None:
    monkeypatch.setattr(
        runner.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(
            {"role": "editor", "username": "msrp-cron"},
        ),
    )

    result = runner._verify_write_auth("https://example.test/v1")

    assert result == {
        "ok": True,
        "status": "ok",
        "role": "editor",
        "user": "msrp-cron",
    }


def test_run_scrape_stops_before_extraction_when_write_auth_fails(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        runner,
        "_verify_write_auth",
        lambda *args, **kwargs: {
            "ok": False,
            "status": "auth_failed",
            "reason": "write_role_required",
            "role": "viewer",
            "requiredRole": "editor",
        },
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError("source resolution should not run")

    monkeypatch.setattr(runner, "_resolve_source_codes", fail_if_called)

    summary = runner.run_scrape(
        ["source-that-should-not-load"],
        api_base="https://example.test/v1",
        dry_run=False,
    )

    assert summary["ok"] is False
    assert summary["sources"] == {}
    assert summary["auth"] == {
        "ok": False,
        "status": "auth_failed",
        "reason": "write_role_required",
        "role": "viewer",
        "requiredRole": "editor",
    }


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
    assert payload["observations"][0]["price_semantics"] == "lease_monthly"
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
    assert payload["observations"][0]["price_semantics"] is None
    assert source_context["rawPayload"]["pricingContext"]["monthly_payment"] == 5990
    assert source_context["pricingContext"] == {
        "monthly_payment": 5990,
        "price_semantics": "lease_monthly",
    }
