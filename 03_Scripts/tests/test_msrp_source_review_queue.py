from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "msrp_source_review_queue.py"


def load_module():
    module_name = "msrp_source_review_queue_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


queue_script = load_module()


def sample_backlog() -> dict:
    return {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "runId": "msrp-dryrun-20260708-043215",
        "groups": [
            {
                "failureReason": "validation_rejected_all",
                "priorityBand": "high",
                "priorityScore": 45.0,
                "recommendedAction": "repair_source_definition",
                "recommendedStrategy": "check price parsing",
                "sourceRepairIssues": [
                    {
                        "countryCode": "fr",
                        "sourceCode": "toyota_yaris_cross_fr_draft_scrapling",
                        "brand": "TOYOTA",
                        "host": "toyota.fr",
                        "sourceUrl": "https://www.toyota.fr/yaris-cross",
                        "status": "dry_run",
                        "valid": 0,
                        "extracted": 2,
                        "failureReason": "validation_rejected_all",
                        "recommendedStrategy": "check price parsing",
                    }
                ],
                "businessResolutionIssues": [],
                "transientRegressions": [],
            },
            {
                "failureReason": "network_unavailable",
                "priorityBand": "recheck",
                "priorityScore": 10.5,
                "recommendedAction": "recheck_before_source_repair",
                "recommendedStrategy": "retry_network_or_proxy",
                "referenceAssist": {
                    "preferred": "official_proxy_or_configurator_api",
                    "thirdPartyReference": "EVKX",
                    "referencePolicy": "reference_only_review_required",
                    "officialSourceRequiredForIngest": True,
                    "acceptanceRules": [
                        "Do not count EVKX as an official MSRP dryrun pass.",
                    ],
                },
                "sourceRepairIssues": [],
                "businessResolutionIssues": [],
                "transientRegressions": [
                    {
                        "countryCode": "nl",
                        "sourceCode": "tesla_model_y_nl_draft_scrapling",
                        "brand": "TESLA",
                        "host": "tesla.com",
                        "sourceUrl": "https://www.tesla.com/nl_nl/modely",
                        "status": "empty",
                        "valid": 0,
                        "extracted": 0,
                        "failureReason": "network_unavailable",
                        "recommendedStrategy": "retry_network_or_proxy",
                        "errorSnippet": "Page.goto: net::ERR_CONNECTION_CLOSED",
                    }
                ],
            },
        ],
    }


def sample_reference_evidence() -> dict:
    return {
        "schemaVersion": "msrp_source_reference_evidence_v1",
        "generatedAt": "2026-07-08T04:40:00Z",
        "backlogRunId": "msrp-dryrun-20260708-043215",
        "referencePolicy": "reference_only_review_required",
        "officialIngestEligible": False,
        "items": [
            {
                "countryCode": "nl",
                "pricingCountry": "Netherlands",
                "brand": "TESLA",
                "modelQuery": "Tesla Model Y",
                "sourceCodes": ["tesla_model_y_nl_draft_scrapling"],
                "referenceSource": "EVKX",
                "referencePolicy": "reference_only_review_required",
                "officialIngestEligible": False,
                "reviewRecommendation": "use_as_review_reference_only",
                "localPriceReferences": [
                    {
                        "name": "Tesla Model Y Standard",
                        "startPrice": 40990,
                        "currency": "EUR",
                        "pricingCountry": "Netherlands",
                        "isConverted": False,
                    },
                    {
                        "name": "Tesla Model Y Performance",
                        "startPrice": 62990,
                        "currency": "EUR",
                        "pricingCountry": "Netherlands",
                        "isConverted": False,
                    },
                ],
                "referenceAssist": {
                    "preferred": "official_proxy_or_configurator_api",
                    "thirdPartyReference": "EVKX",
                    "referencePolicy": "reference_only_review_required",
                    "officialSourceRequiredForIngest": True,
                    "acceptanceRules": [
                        "Only use EVKX records when pricingCountry matches the target country.",
                    ],
                },
            }
        ],
    }


def test_build_source_review_queue_merges_backlog_and_reference_evidence() -> None:
    payload = queue_script.build_source_review_queue(
        sample_backlog(),
        sample_reference_evidence(),
    )

    assert payload["schemaVersion"] == "msrp_source_review_queue_v1"
    assert payload["officialSourceRequiredForIngest"] is True
    assert payload["officialIngestEligible"] is False
    assert payload["summary"] == {
        "totalCases": 2,
        "sourceRepairCount": 1,
        "businessResolutionCount": 0,
        "transientRecheckCount": 1,
        "referenceOnlyCount": 1,
        "officialSourceRequiredCount": 2,
        "officialIngestEligibleCount": 0,
        "localReferenceCount": 2,
        "countryCount": 2,
        "countries": ["FR", "NL"],
    }

    repair, tesla = payload["items"]
    assert repair["queueType"] == "source_repair"
    assert repair["sourceCode"] == "toyota_yaris_cross_fr_draft_scrapling"
    assert repair["localReferenceCount"] == 0

    assert tesla["queueType"] == "transient_recheck"
    assert tesla["sourceCode"] == "tesla_model_y_nl_draft_scrapling"
    assert tesla["referencePolicy"] == "reference_only_review_required"
    assert tesla["referenceSource"] == "EVKX"
    assert tesla["officialSourceRequiredForIngest"] is True
    assert tesla["officialIngestEligible"] is False
    assert tesla["localReferenceCount"] == 2
    assert tesla["reviewRecommendation"] == "use_as_review_reference_only"
    assert tesla["evidence"]["backlogRunId"] == "msrp-dryrun-20260708-043215"


def test_build_source_review_queue_ignores_stale_reference_evidence() -> None:
    stale_reference = {
        **sample_reference_evidence(),
        "backlogRunId": "msrp-dryrun-older",
    }

    payload = queue_script.build_source_review_queue(
        sample_backlog(),
        stale_reference,
    )

    assert payload["warnings"] == ["reference_evidence_run_mismatch"]
    assert payload["summary"]["referenceOnlyCount"] == 1
    assert payload["summary"]["localReferenceCount"] == 0
    tesla = payload["items"][1]
    assert tesla["sourceCode"] == "tesla_model_y_nl_draft_scrapling"
    assert tesla["referenceSource"] == "EVKX"
    assert tesla["referencePolicy"] == "reference_only_review_required"
    assert tesla["localReferenceCount"] == 0
    assert tesla["reviewRecommendation"] == "repair_official_source"


def test_build_source_review_queue_reads_sample_and_top_level_backlog_items() -> None:
    backlog = {
        "schemaVersion": "msrp_source_repair_backlog_v1",
        "runId": "msrp-dryrun-20260709-081500",
        "sourceIssues": [
            {
                "countryCode": "pt",
                "sourceCode": "volvo_xc40_pt_draft_scrapling",
                "brand": "VOLVO",
                "failureReason": "no_observation_extracted",
                "recommendedAction": "repair_source_definition",
            }
        ],
        "businessResolutionIssues": [
            {
                "countryCode": "es",
                "sourceCode": "mg_hs_es_draft_scrapling",
                "brand": "MG",
                "failureReason": "offer_price_only",
                "recommendedAction": "business_resolution_required",
            }
        ],
        "transientSourceRegressions": [
            {
                "countryCode": "nl",
                "sourceCode": "tesla_model_y_nl_draft_scrapling",
                "brand": "TESLA",
                "failureReason": "network_unavailable",
                "recommendedAction": "recheck_before_source_repair",
            },
            {
                "countryCode": "se",
                "sourceCode": "volvo_xc60_se_draft_scrapling",
                "brand": "VOLVO",
                "failureReason": "network_unavailable",
                "recommendedAction": "recheck_before_source_repair",
            },
        ],
        "groups": [
            {
                "failureReason": "network_unavailable",
                "priorityBand": "recheck",
                "priorityScore": 11.0,
                "recommendedAction": "recheck_before_source_repair",
                "recommendedStrategy": "retry_network_or_proxy",
                "sourceRepairIssues": [],
                "businessResolutionIssues": [],
                "sampleTransientRegressions": [
                    {
                        "countryCode": "nl",
                        "sourceCode": "tesla_model_y_nl_draft_scrapling",
                        "brand": "TESLA",
                        "failureReason": "network_unavailable",
                        "recommendedAction": "recheck_before_source_repair",
                    }
                ],
            }
        ],
    }

    payload = queue_script.build_source_review_queue(backlog)

    assert payload["summary"]["sourceRepairCount"] == 1
    assert payload["summary"]["businessResolutionCount"] == 1
    assert payload["summary"]["transientRecheckCount"] == 2
    assert payload["summary"]["totalCases"] == 4
    by_source = {
        item["sourceCode"]: item
        for item in payload["items"]
    }
    assert by_source["volvo_xc40_pt_draft_scrapling"]["queueType"] == "source_repair"
    assert by_source["mg_hs_es_draft_scrapling"]["queueType"] == "business_resolution"
    assert by_source["tesla_model_y_nl_draft_scrapling"]["queueType"] == "transient_recheck"
    assert by_source["volvo_xc60_se_draft_scrapling"]["queueType"] == "transient_recheck"


def test_run_writes_json_and_markdown(tmp_path: Path) -> None:
    backlog_path = tmp_path / "backlog.json"
    reference_path = tmp_path / "reference.json"
    backlog_path.write_text(json.dumps(sample_backlog()), encoding="utf-8")
    reference_path.write_text(json.dumps(sample_reference_evidence()), encoding="utf-8")

    payload = queue_script.run(
        backlog_path=str(backlog_path),
        reference_path=str(reference_path),
        out_dir=str(tmp_path),
    )

    json_path = tmp_path / "msrp_source_review_queue.json"
    md_path = tmp_path / "msrp_source_review_queue.md"
    assert json_path.exists()
    assert md_path.exists()
    assert json.loads(json_path.read_text(encoding="utf-8"))["summary"][
        "totalCases"
    ] == 2
    markdown = md_path.read_text(encoding="utf-8")
    assert "Policy: third-party references are review-only" in markdown
    assert "tesla_model_y_nl_draft_scrapling" in markdown
    assert payload["summary"]["referenceOnlyCount"] == 1
