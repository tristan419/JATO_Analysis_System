from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "msrp_source_repair_backlog.py"


def load_module():
    module_name = "msrp_source_repair_backlog_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


backlog_script = load_module()


def test_v3_report_uses_scored_repair_backlog(tmp_path: Path) -> None:
    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260616-101010",
        "results": [
            {
                "country": "se",
                "sourceCode": "volvo_xc60_se_draft_scrapling",
                "status": "empty",
                "valid": 0,
                "failureReason": "no_observation_extracted",
                "recommendedStrategy": "diagnose_with_msrp_page_analyzer",
                "sourceUrl": "https://www.volvocars.com/se/build/xc60-hybrid/",
            },
            {
                "country": "fi",
                "sourceCode": "volvo_xc60_fi_draft_scrapling",
                "status": "empty",
                "valid": 0,
                "failureReason": "no_observation_extracted",
                "recommendedStrategy": "diagnose_with_msrp_page_analyzer",
                "sourceUrl": "https://www.volvocars.com/fi/build/xc60-hybrid/",
            },
        ],
    }
    report_path = tmp_path / "dryrun_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    backlog = backlog_script.run(str(report_path), str(tmp_path))

    assert backlog["schemaVersion"] == "msrp_source_repair_backlog_v1"
    assert backlog["runId"] == "msrp-dryrun-20260616-101010"
    assert backlog["sourceRepairIssueCount"] == 2
    assert backlog["transientRegressionCount"] == 0
    assert [item["sourceCode"] for item in backlog["sourceIssues"]] == [
        "volvo_xc60_se_draft_scrapling",
        "volvo_xc60_fi_draft_scrapling",
    ]
    assert backlog["transientSourceRegressions"] == []
    assert backlog["topSourceHosts"][0]["host"] == "volvocars.com"
    group = backlog["groups"][0]
    assert group["failureReason"] == "no_observation_extracted"
    assert [item["countryCode"] for item in group["sourceRepairIssues"]] == [
        "se",
        "fi",
    ]
    assert group["priorityScore"] > 0
    assert group["priorityBand"] in {"medium", "high", "critical"}
    assert group["reviewAssist"]["preferred"] == "rule_based_then_llm"
    assert group["reviewAssist"]["llmFit"] == "medium"
    assert group["reviewAssist"]["neuralNetworkFit"] == "not_recommended_until_labeled_corpus"


def test_v3_report_can_build_backlog_from_country_details_only(tmp_path: Path) -> None:
    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260617-095029",
        "countriesDetail": [
            {
                "countryCode": "at",
                "sources": [
                    {
                        "sourceCode": "audi_q8_at_draft_scrapling",
                        "brand": "AUDI",
                        "status": "empty",
                        "valid": 0,
                        "extracted": 1,
                        "rejected": 1,
                        "failureReason": "source_url_not_found",
                        "recommendedStrategy": "update_source_url",
                        "sourceUrl": "https://www.audi.at/modelle/q8",
                        "httpStatus": 404,
                        "rejectedReasons": [
                            "msrp_value=229.0 < 5000.0 for base_msrp",
                        ],
                        "rejectedRules": ["price_range"],
                        "rejectionRuleCounts": {"price_range": 1},
                        "sampleRejectedObservations": [
                            {
                                "officialModel": "Q8",
                                "officialTrim": "Accessory",
                                "msrpValue": 229,
                            },
                        ],
                    },
                    {
                        "sourceCode": "byd_seal_u_at_draft_scrapling",
                        "brand": "BYD",
                        "status": "empty",
                        "valid": 0,
                        "failureReason": "source_url_not_found",
                        "recommendedStrategy": "update_source_url",
                        "sourceUrl": "https://www.byd.com/at/car/seal-u",
                        "httpStatus": 404,
                    },
                ],
            }
        ],
    }
    report_path = tmp_path / "dryrun_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    backlog = backlog_script.run(str(report_path), str(tmp_path))

    assert backlog["runId"] == "msrp-dryrun-20260617-095029"
    assert backlog["sourceRepairIssueCount"] == 2
    group = backlog["groups"][0]
    assert group["failureReason"] == "source_url_not_found"
    assert group["recommendedStrategy"] == "update_source_url"
    assert group["affectedCountries"] == ["at"]
    assert group["affectedBrands"] == ["AUDI", "BYD"]
    assert [item["host"] for item in group["sourceRepairIssues"]] == [
        "audi.at",
        "byd.com",
    ]
    audi_issue = backlog["sourceIssues"][0]
    assert audi_issue["rejectedRules"] == ["price_range"]
    assert audi_issue["rejectionRuleCounts"] == {"price_range": 1}
    assert (
        audi_issue["sampleRejectedObservations"][0]["officialTrim"]
        == "Accessory"
    )


def test_v3_report_marks_historical_pass_as_recheck(tmp_path: Path) -> None:
    current_run_id = "msrp-dryrun-20260616-101010"
    previous_run_id = "msrp-dryrun-20260615-101010"
    source_code = "volvo_xc60_se_draft_scrapling"
    previous_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": previous_run_id,
        "countriesDetail": [
            {
                "countryCode": "se",
                "sources": [
                    {
                        "sourceCode": source_code,
                        "status": "pass",
                        "valid": 1,
                    }
                ],
            }
        ],
        "generatedAt": "2026-06-15T10:10:10Z",
    }
    previous_path = tmp_path / f"dryrun_report_{previous_run_id}.json"
    previous_path.write_text(json.dumps(previous_report), encoding="utf-8")
    (tmp_path / "dryrun_runs_index.json").write_text(
        json.dumps({
            "schemaVersion": "msrp_dryrun_runs_index_v1",
            "latestRunId": previous_run_id,
            "runs": [
                {
                    "runId": previous_run_id,
                    "finishedAt": "2026-06-15T10:12:00Z",
                    "artifactPath": str(previous_path),
                }
            ],
        }),
        encoding="utf-8",
    )
    current_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": current_run_id,
        "results": [
            {
                "country": "se",
                "sourceCode": source_code,
                "status": "empty",
                "valid": 0,
                "failureReason": "http_timeout",
                "recommendedStrategy": "retry_network_or_proxy",
            }
        ],
    }
    current_path = tmp_path / "dryrun_report.json"
    current_path.write_text(json.dumps(current_report), encoding="utf-8")

    backlog = backlog_script.run(str(current_path), str(tmp_path))

    assert backlog["sourceRepairIssueCount"] == 0
    assert backlog["transientRegressionCount"] == 1
    assert backlog["sourceIssues"] == []
    assert backlog["transientSourceRegressions"][0]["sourceCode"] == source_code
    assert (
        backlog["transientSourceRegressions"][0]["recommendedAction"]
        == "recheck_before_source_repair"
    )
    group = backlog["groups"][0]
    assert group["priorityBand"] == "recheck"
    assert group["recommendedAction"] == "recheck_before_source_repair"
    assert group["reviewAssist"]["preferred"] == "rule_based_recheck"
    assert group["sampleTransientRegressions"][0]["lastKnownGoodRunId"] == previous_run_id
    assert group["transientRegressions"][0]["lastKnownGoodRunId"] == previous_run_id


def test_v3_report_marks_dynamic_price_not_ready_as_recheck_without_history(
    tmp_path: Path,
) -> None:
    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260618-101010",
        "results": [
            {
                "country": "fi",
                "sourceCode": "volkswagen_tiguan_fi_draft_scrapling",
                "brand": "VOLKSWAGEN",
                "status": "empty",
                "valid": 0,
                "failureReason": "dynamic_price_not_ready",
                "recommendedStrategy": "retry_or_reduce_concurrency",
                "sourceUrl": "https://www.volkswagen.fi/fi/rakenna-auto.html",
            }
        ],
    }
    report_path = tmp_path / "dryrun_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    backlog = backlog_script.run(str(report_path), str(tmp_path))

    assert backlog["sourceRepairIssueCount"] == 0
    assert backlog["transientRegressionCount"] == 1
    assert backlog["sourceIssues"] == []
    transient = backlog["transientSourceRegressions"][0]
    assert transient["sourceCode"] == "volkswagen_tiguan_fi_draft_scrapling"
    assert transient["recommendedAction"] == "recheck_before_source_repair"
    assert transient["transientRegression"] is True
    assert "lastKnownGoodRunId" not in transient
    group = backlog["groups"][0]
    assert group["failureReason"] == "dynamic_price_not_ready"
    assert group["priorityBand"] == "recheck"
    assert group["sourceRepairIssueCount"] == 0


def test_v3_report_uses_artifact_history_when_output_dir_differs(
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "artifacts"
    output_dir = tmp_path / "reports"
    artifact_dir.mkdir()
    current_run_id = "msrp-dryrun-20260616-101010"
    previous_run_id = "msrp-dryrun-20260615-101010"
    source_code = "volvo_xc60_se_draft_scrapling"
    previous_path = artifact_dir / f"dryrun_report_{previous_run_id}.json"
    previous_path.write_text(
        json.dumps({
            "schemaVersion": "msrp_dryrun_report_v3",
            "runId": previous_run_id,
            "countriesDetail": [
                {
                    "countryCode": "se",
                    "sources": [
                        {
                            "sourceCode": source_code,
                            "status": "pass",
                            "valid": 1,
                        }
                    ],
                }
            ],
        }),
        encoding="utf-8",
    )
    (artifact_dir / "dryrun_runs_index.json").write_text(
        json.dumps({
            "schemaVersion": "msrp_dryrun_runs_index_v1",
            "runs": [
                {
                    "runId": previous_run_id,
                    "finishedAt": "2026-06-15T10:12:00Z",
                    "artifactPath": str(previous_path),
                }
            ],
        }),
        encoding="utf-8",
    )
    current_path = artifact_dir / "dryrun_report.json"
    current_path.write_text(
        json.dumps({
            "schemaVersion": "msrp_dryrun_report_v3",
            "runId": current_run_id,
            "results": [
                {
                    "country": "se",
                    "sourceCode": source_code,
                    "status": "empty",
                    "valid": 0,
                    "failureReason": "http_timeout",
                    "recommendedStrategy": "retry_network_or_proxy",
                }
            ],
        }),
        encoding="utf-8",
    )

    backlog = backlog_script.run(str(current_path), str(output_dir))

    assert (output_dir / "msrp_source_repair_backlog.json").exists()
    assert backlog["sourceRepairIssueCount"] == 0
    assert backlog["transientRegressionCount"] == 1
    assert backlog["transientSourceRegressions"][0]["lastKnownGoodRunId"] == previous_run_id


def test_v3_report_splits_business_resolution_from_source_repair(
    tmp_path: Path,
) -> None:
    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260623-082923",
        "results": [
            {
                "country": "no",
                "sourceCode": "mercedes_eqb_no_draft_scrapling",
                "brand": "MERCEDES-BENZ",
                "status": "empty",
                "valid": 0,
                "failureReason": "model_not_currently_available",
                "recommendedStrategy": "exclude_or_replace_discontinued_model",
                "sourceUrl": "https://www.mercedes-benz.no/passengercars/models/suv/eqb/overview.html",
                "finalUrl": "https://www.mercedes-benz.no/our-brands/eqb-ikke-tilgjengelig/",
            },
            {
                "country": "no",
                "sourceCode": "toyota_yaris_cross_no_draft_scrapling",
                "brand": "TOYOTA",
                "status": "empty",
                "valid": 0,
                "failureReason": "source_url_redirected_to_homepage",
                "recommendedStrategy": "update_source_url_or_confirm_model_availability",
                "sourceUrl": "https://www.toyota.no/biler/yaris-cross",
                "finalUrl": "https://www.toyota.no/",
            },
        ],
    }
    report_path = tmp_path / "dryrun_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    backlog = backlog_script.run(str(report_path), str(tmp_path))

    assert backlog["totalIssueCount"] == 2
    assert backlog["sourceRepairIssueCount"] == 1
    assert backlog["businessResolutionCount"] == 1
    assert backlog["transientRegressionCount"] == 0
    assert [item["sourceCode"] for item in backlog["sourceIssues"]] == [
        "toyota_yaris_cross_no_draft_scrapling",
    ]
    assert [item["sourceCode"] for item in backlog["businessResolutionIssues"]] == [
        "mercedes_eqb_no_draft_scrapling",
    ]

    toyota_issue = backlog["sourceIssues"][0]
    assert toyota_issue["sourceRepairIssue"] is True
    assert toyota_issue["businessResolution"] is False
    assert toyota_issue["recommendedAction"] == "repair_source_definition"

    business_issue = backlog["businessResolutionIssues"][0]
    assert business_issue["sourceRepairIssue"] is False
    assert business_issue["businessResolution"] is True
    assert business_issue["recommendedAction"] == "business_resolution_required"

    groups = {
        group["failureReason"]: group
        for group in backlog["groups"]
    }
    business_group = groups["model_not_currently_available"]
    assert business_group["sourceRepairIssueCount"] == 0
    assert business_group["businessResolutionCount"] == 1
    assert business_group["priorityBand"] == "business"
    assert business_group["recommendedAction"] == "business_resolution_required"
    assert business_group["reviewAssist"]["preferred"] == "business_rule_review"

    homepage_group = groups["source_url_redirected_to_homepage"]
    assert homepage_group["sourceRepairIssueCount"] == 1
    assert homepage_group["businessResolutionCount"] == 0

    markdown = (tmp_path / "msrp_source_repair_backlog.md").read_text(
        encoding="utf-8"
    )
    assert "Business resolutions: 1" in markdown
    assert "## Business Resolution Queue" in markdown
    assert "mercedes_eqb_no_draft_scrapling" in markdown


def test_v3_report_marks_tesla_403_with_evkx_reference_policy(tmp_path: Path) -> None:
    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260617-101010",
        "results": [
            {
                "country": "se",
                "code": "tesla_model_y_se_draft_scrapling",
                "brand": "TESLA",
                "status": "empty",
                "valid": 0,
                "failureReason": "forbidden_403",
                "recommendedStrategy": "manual_review_or_proxy_required",
                "sourceUrl": "https://www.tesla.com/sv_se/modely",
                "httpStatus": 403,
            },
            {
                "country": "fi",
                "code": "tesla_model_y_fi_draft_scrapling",
                "brand": "TESLA",
                "status": "empty",
                "valid": 0,
                "failureReason": "forbidden_403",
                "recommendedStrategy": "manual_review_or_proxy_required",
                "sourceUrl": "https://www.tesla.com/fi_FI/modely",
                "httpStatus": 403,
            },
        ],
    }
    report_path = tmp_path / "dryrun_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    backlog = backlog_script.run(str(report_path), str(tmp_path))

    group = backlog["groups"][0]
    assert group["failureReason"] == "forbidden_403"
    assert group["affectedBrands"] == ["TESLA"]
    assert len(backlog["sourceIssues"]) == 2
    assert backlog["sourceIssues"][0]["host"] == "tesla.com"
    assert backlog["sourceIssues"][0]["recommendedAction"] == "repair_source_definition"
    assert group["referenceAssist"]["preferred"] == "official_proxy_or_configurator_api"
    assert group["referenceAssist"]["thirdPartyReference"] == "EVKX"
    assert group["referenceAssist"]["referencePolicy"] == "reference_only_review_required"
    assert group["referenceAssist"]["officialSourceRequiredForIngest"] is True
    assert "isConverted is false" in " ".join(
        group["referenceAssist"]["acceptanceRules"]
    )

    markdown = (tmp_path / "msrp_source_repair_backlog.md").read_text(
        encoding="utf-8"
    )
    assert "EVKX reference_only_review_required" in markdown


def test_v3_report_marks_tesla_anti_bot_with_evkx_reference_policy(
    tmp_path: Path,
) -> None:
    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260624-135526",
        "results": [
            {
                "country": "fr",
                "code": "tesla_model_y_fr_draft_scrapling",
                "brand": "TESLA",
                "status": "empty",
                "valid": 0,
                "extracted": 0,
                "failureReason": "anti_bot_access_denied",
                "recommendedStrategy": "manual_review_or_proxy_required",
                "sourceUrl": "https://www.tesla.com/fr_FR/modely",
                "httpStatus": 403,
                "extractorError": (
                    "anti_bot_access_denied: Access Denied You don't have "
                    "permission to access https://www.tesla.com/fr_FR/modely "
                    "on this server."
                ),
            },
        ],
    }
    report_path = tmp_path / "dryrun_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    backlog = backlog_script.run(str(report_path), str(tmp_path))

    group = backlog["groups"][0]
    assert group["failureReason"] == "anti_bot_access_denied"
    assert group["affectedBrands"] == ["TESLA"]
    assert backlog["sourceIssues"][0]["errorSnippet"].startswith(
        "anti_bot_access_denied: Access Denied"
    )
    assert group["referenceAssist"]["preferred"] == "official_proxy_or_configurator_api"
    assert group["referenceAssist"]["thirdPartyReference"] == "EVKX"
    assert group["referenceAssist"]["referencePolicy"] == "reference_only_review_required"
    assert group["referenceAssist"]["officialSourceRequiredForIngest"] is True

    markdown = (tmp_path / "msrp_source_repair_backlog.md").read_text(
        encoding="utf-8"
    )
    assert "anti_bot_access_denied" in markdown
    assert "EVKX reference_only_review_required" in markdown


def test_v3_report_marks_tesla_network_unavailable_with_evkx_reference_policy(
    tmp_path: Path,
) -> None:
    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-20260708-043215",
        "results": [
            {
                "country": "nl",
                "code": "tesla_model_y_nl_draft_scrapling",
                "brand": "TESLA",
                "status": "empty",
                "valid": 0,
                "extracted": 0,
                "failureReason": "network_unavailable",
                "recommendedStrategy": "retry_network_or_proxy",
                "sourceUrl": "https://www.tesla.com/nl_nl/modely",
                "extractorError": (
                    "Error: Page.goto: net::ERR_CONNECTION_CLOSED at "
                    "https://www.tesla.com/nl_nl/modely"
                ),
            },
        ],
    }
    report_path = tmp_path / "dryrun_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    backlog = backlog_script.run(str(report_path), str(tmp_path))

    group = backlog["groups"][0]
    assert group["failureReason"] == "network_unavailable"
    assert group["affectedBrands"] == ["TESLA"]
    assert group["sourceRepairIssueCount"] == 0
    assert group["transientRegressionCount"] == 1
    assert group["recommendedAction"] == "recheck_before_source_repair"
    assert backlog["transientSourceRegressions"][0]["host"] == "tesla.com"
    assert group["referenceAssist"]["preferred"] == "official_proxy_or_configurator_api"
    assert group["referenceAssist"]["thirdPartyReference"] == "EVKX"
    assert group["referenceAssist"]["referencePolicy"] == "reference_only_review_required"
    assert group["referenceAssist"]["officialSourceRequiredForIngest"] is True

    markdown = (tmp_path / "msrp_source_repair_backlog.md").read_text(
        encoding="utf-8"
    )
    assert "network_unavailable" in markdown
    assert "EVKX reference_only_review_required" in markdown


def test_legacy_report_keeps_summary_backlog_format(tmp_path: Path) -> None:
    report = {
        "total": 1,
        "pass": 0,
        "passPct": 0.0,
        "results": [
            {
                "country": "se",
                "code": "volvo_xc60_se_draft_scrapling",
                "status": "empty",
                "valid": 0,
                "failureReason": "validation_rejected_all",
                "rejectedReasons": ["currency missing"],
            }
        ],
    }
    report_path = tmp_path / "legacy_dryrun_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    backlog = backlog_script.run(str(report_path), str(tmp_path))

    assert backlog["summary"]["failedCount"] == 1
    assert backlog["failureBreakdown"] == {"validation_rejected_all": 1}
    assert backlog["backlog"][0]["recommendedStrategy"] == "check default_currency in the source YAML"


def test_legacy_report_classifies_price_floor_rejection(tmp_path: Path) -> None:
    report = {
        "total": 1,
        "pass": 0,
        "passPct": 0.0,
        "results": [
            {
                "country": "fr",
                "code": "nissan_qashqai_fr_draft_scrapling",
                "status": "dry_run",
                "valid": 0,
                "extracted": 1,
                "rejected": 1,
                "failureReason": "validation_rejected_all",
                "rejectedReasons": [
                    "msrp_value=229.0 < 5000.0 for base_msrp",
                ],
                "rejectionRuleCounts": {"price_range": 1},
            }
        ],
    }
    report_path = tmp_path / "legacy_dryrun_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")

    backlog = backlog_script.run(str(report_path), str(tmp_path))

    assert backlog["backlog"][0]["likelyCause"] == "price range issue"
    assert (
        backlog["backlog"][0]["recommendedStrategy"]
        == "check price parsing and units in extraction config"
    )
