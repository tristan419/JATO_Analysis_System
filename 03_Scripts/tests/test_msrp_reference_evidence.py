from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "msrp_reference_evidence.py"


def load_module():
    module_name = "msrp_reference_evidence_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


reference_script = load_module()


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self.headers = {}
        self._responses = list(responses)
        self.calls = []

    def post(self, url, json, timeout):
        self.calls.append((url, json, timeout))
        return _FakeResponse(self._responses.pop(0))


def _tesla_backlog():
    return {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "runId": "msrp-dryrun-20260617-012812",
        "groups": [
            {
                "failureReason": "forbidden_403",
                "recommendedStrategy": "manual_review_or_proxy_required",
                "affectedBrands": ["TESLA"],
                "sampleSources": [
                    "tesla_model_y_fi_draft_scrapling",
                    "tesla_model_y_se_draft_scrapling",
                ],
                "referenceAssist": {
                    "thirdPartyReference": "EVKX",
                    "referencePolicy": "reference_only_review_required",
                    "officialSourceRequiredForIngest": True,
                },
            }
        ],
    }


def test_source_model_query_parses_brand_model_and_country_suffix() -> None:
    assert (
        reference_script._source_model_query(
            "tesla_model_y_se_draft_scrapling",
            brand="TESLA",
            country_code="se",
        )
        == "Tesla Model Y"
    )


def test_build_reference_evidence_filters_to_local_non_converted_prices() -> None:
    session = _FakeSession(
        [
            {
                "evs": [
                    {
                        "evId": "fi-standard",
                        "name": "Tesla Model Y Standard",
                        "startPrice": 39990,
                        "currency": "EUR",
                        "pricingCountry": "Finland",
                        "isConverted": False,
                        "infoUri": "../models/tesla/model_y/model_y_standard/",
                    },
                    {
                        "evId": "fi-converted",
                        "name": "Tesla Model Y RWD",
                        "startPrice": 434152,
                        "currency": "EUR",
                        "pricingCountry": "Australia",
                        "isConverted": True,
                    },
                ],
                "hasNextPage": False,
            },
            {
                "evs": [
                    {
                        "evId": "se-standard",
                        "name": "Tesla Model Y Standard",
                        "startPrice": 499990,
                        "currency": "SEK",
                        "pricingCountry": "Sweden",
                        "isConverted": False,
                        "infoUri": "../models/tesla/model_y/model_y_standard/",
                    },
                    {
                        "evId": "se-converted",
                        "name": "Tesla Model Y RWD",
                        "startPrice": 434152,
                        "currency": "SEK",
                        "pricingCountry": "Sweden",
                        "isConverted": True,
                    },
                ],
                "hasNextPage": False,
            },
        ]
    )

    payload = reference_script.build_reference_evidence(
        _tesla_backlog(),
        session=session,
        page_size=1000,
        max_pages=1,
    )

    assert payload["schemaVersion"] == "msrp_source_reference_evidence_v1"
    assert payload["officialIngestEligible"] is False
    assert payload["summary"] == {
        "evidenceItemCount": 2,
        "localReferenceCount": 2,
        "missingLocalReferenceCount": 0,
        "officialIngestEligibleCount": 0,
    }
    assert [call[1]["pricingCountry"] for call in session.calls] == [
        "Finland",
        "Sweden",
    ]
    assert [
        item["localPriceReferences"][0]["evId"]
        for item in payload["items"]
    ] == ["fi-standard", "se-standard"]
    assert all(item["officialIngestEligible"] is False for item in payload["items"])
    assert {
        item["reviewRecommendation"] for item in payload["items"]
    } == {"use_as_review_reference_only"}


def test_build_reference_evidence_uses_per_source_tesla_issue_for_austria() -> None:
    backlog = {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "runId": "msrp-dryrun-20260618-110029",
        "sourceIssues": [
            {
                "countryCode": "at",
                "sourceCode": "mg_zs_at_draft_scrapling",
                "brand": "MG",
                "host": "mgmotor.at",
                "failureReason": "network_unavailable",
            },
            {
                "countryCode": "at",
                "sourceCode": "tesla_model_y_at_draft_scrapling",
                "brand": "TESLA",
                "host": "tesla.com",
                "failureReason": "network_unavailable",
                "recommendedStrategy": "retry_network_or_proxy",
            },
        ],
    }
    session = _FakeSession([
        {
            "evs": [
                {
                    "evId": "at-standard",
                    "name": "Tesla Model Y Standard",
                    "startPrice": 44990,
                    "currency": "EUR",
                    "pricingCountry": "Austria",
                    "isConverted": False,
                    "infoUri": "../models/tesla/model_y/model_y_standard/",
                },
                {
                    "evId": "at-converted",
                    "name": "Tesla Model Y RWD",
                    "startPrice": 44990,
                    "currency": "EUR",
                    "pricingCountry": "Germany",
                    "isConverted": True,
                },
            ],
            "hasNextPage": False,
        },
    ])

    payload = reference_script.build_reference_evidence(
        backlog,
        session=session,
        page_size=1000,
        max_pages=1,
    )

    assert payload["summary"] == {
        "evidenceItemCount": 1,
        "localReferenceCount": 1,
        "missingLocalReferenceCount": 0,
        "officialIngestEligibleCount": 0,
    }
    assert [call[1]["pricingCountry"] for call in session.calls] == ["Austria"]
    item = payload["items"][0]
    assert item["countryCode"] == "at"
    assert item["brand"] == "TESLA"
    assert item["modelQuery"] == "Tesla Model Y"
    assert item["sourceCodes"] == ["tesla_model_y_at_draft_scrapling"]
    assert item["officialIngestEligible"] is False
    assert item["localPriceReferences"][0]["evId"] == "at-standard"


def test_build_reference_evidence_marks_missing_local_references() -> None:
    session = _FakeSession([
        {"evs": [], "hasNextPage": False},
        {"evs": [], "hasNextPage": False},
    ])

    payload = reference_script.build_reference_evidence(
        _tesla_backlog(),
        session=session,
        page_size=1000,
        max_pages=1,
    )

    assert payload["summary"]["localReferenceCount"] == 0
    assert payload["summary"]["missingLocalReferenceCount"] == 2
    assert {
        item["reviewRecommendation"] for item in payload["items"]
    } == {"continue_official_source_repair"}
