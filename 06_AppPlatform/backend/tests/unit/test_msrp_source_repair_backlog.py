"""Tests for the structured MSRP source-repair backlog."""

import importlib.util
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT_PATH = REPO_ROOT / "03_Scripts" / "msrp_source_repair_backlog.py"

spec = importlib.util.spec_from_file_location(
    "msrp_source_repair_backlog_test",
    SCRIPT_PATH,
)
backlog_mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(backlog_mod)


def test_build_backlog_explains_source_lifecycle_access_and_extractor_failures() -> None:
    report = {
        "total": 4,
        "pass": 0,
        "passPct": 0.0,
        "results": [
            {
                "country": "no",
                "code": "mercedes_eqb_no",
                "status": "empty",
                "failureReason": "json_ld_empty",
                "recommendedStrategy": "try_css_or_attr_json",
                "sourceUrl": "https://www.mercedes-benz.no/cars/eqb",
                "finalUrl": (
                    "https://www.mercedes-benz.no/cars/"
                    "eqb-ikke-tilgjengelig"
                ),
            },
            {
                "country": "no",
                "code": "toyota_yaris_cross_no",
                "status": "empty",
                "failureReason": "json_ld_empty",
                "recommendedStrategy": "try_css_or_attr_json",
                "sourceUrl": "https://www.toyota.no/nybil/yaris-cross",
                "finalUrl": "https://www.toyota.no/",
            },
            {
                "country": "se",
                "code": "tesla_model_y_se",
                "status": "error",
                "failureReason": "http_error",
                "recommendedStrategy": "retry",
                "sourceUrl": "https://www.tesla.com/sv_se/modely",
                "httpStatus": 403,
            },
            {
                "country": "fi",
                "code": "volvo_xc60_fi",
                "status": "empty",
                "failureReason": "no_observation_extracted",
                "recommendedStrategy": "diagnose_with_msrp_page_analyzer",
                "sourceUrl": "https://www.volvocars.com/fi/cars/xc60/",
            },
        ],
    }

    backlog = backlog_mod._build_backlog(report)
    items = {item["failureReason"]: item for item in backlog["backlog"]}

    assert set(items) == {
        "forbidden_403",
        "homepage_redirect",
        "no_observation_extracted",
        "official_model_unavailable",
    }
    assert items["official_model_unavailable"]["issueClass"] == "source_lifecycle"
    assert items["official_model_unavailable"]["originalFailureReason"] == (
        "json_ld_empty"
    )
    assert items["homepage_redirect"]["sourceLifecycleStatus"] == (
        "homepage_redirect"
    )
    assert items["forbidden_403"]["issueClass"] == "access_control"
    assert items["forbidden_403"]["httpStatus"] == 403
    assert items["no_observation_extracted"]["issueClass"] == (
        "extractor_strategy"
    )
    assert backlog["issueClassBreakdown"] == {
        "access_control": 1,
        "extractor_strategy": 1,
        "source_lifecycle": 2,
    }


def test_v3_backlog_keeps_priority_payload_and_adds_structured_summary(
    tmp_path,
) -> None:
    report = {
        "total": 1,
        "pass": 0,
        "results": [
            {
                "country": "se",
                "code": "tesla_model_y_se",
                "status": "error",
                "failureReason": "http_error",
                "recommendedStrategy": "retry",
                "sourceUrl": "https://www.tesla.com/sv_se/modely",
                "httpStatus": 403,
            }
        ],
    }
    json_path = tmp_path / "msrp_source_repair_backlog.json"
    markdown_path = tmp_path / "msrp_source_repair_backlog.md"
    markdown_path.write_text("# Existing priority report\n", encoding="utf-8")

    backlog = backlog_mod._attach_structured_issue_summary(
        {"priorityMetadata": {"kept": True}},
        report,
        json_path,
        markdown_path,
    )

    assert backlog["priorityMetadata"] == {"kept": True}
    assert backlog["structuredIssueSummary"]["failureBreakdown"] == {
        "forbidden_403": 1
    }
    persisted = json.loads(json_path.read_text(encoding="utf-8"))
    assert persisted["priorityMetadata"] == {"kept": True}
    assert "Structured Failure Feedback" in markdown_path.read_text(
        encoding="utf-8"
    )


def test_existing_unavailable_reason_gets_canonical_lifecycle_metadata() -> None:
    issue = backlog_mod.enrich_msrp_source_issue(
        {
            "failureReason": "model_not_currently_available",
            "recommendedStrategy": "business_resolution_required",
        }
    )

    assert issue["failureReason"] == "official_model_unavailable"
    assert issue["originalFailureReason"] == "model_not_currently_available"
    assert issue["issueClass"] == "source_lifecycle"
    assert issue["sourceLifecycleStatus"] == "official_unavailable"
