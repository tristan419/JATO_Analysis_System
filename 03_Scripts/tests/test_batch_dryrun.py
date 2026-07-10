from __future__ import annotations

import importlib.util
import json
import logging
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "batch_dryrun.py"


def load_module():
    module_name = "batch_dryrun_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


batch_dryrun = load_module()


def _slow_run_scrape(*, source_codes, dry_run, source_timeout_seconds):
    time.sleep(5)
    return {
        "sources": {
            source_codes[0]: {
                "status": "dry_run",
                "valid": 1,
                "extracted": 1,
                "rejected": 0,
            }
        }
    }


def test_capture_source_logs_classifies_scrapling_403() -> None:
    root_logger = logging.getLogger()
    scrapling_logger = logging.getLogger("scrapling")
    original_root_level = root_logger.level
    original_scrapling_level = scrapling_logger.level
    original_scrapling_propagate = scrapling_logger.propagate

    root_logger.setLevel(logging.WARNING)
    capture = batch_dryrun._RunLogCapture()
    capture.setFormatter(logging.Formatter("%(levelname)s %(name)s - %(message)s"))

    try:
        with batch_dryrun._capture_source_logs(capture):
            logging.getLogger("scrapling.fetchers").info(
                "Fetched (403) <GET https://www.tesla.com/sv_se/modely>"
            )
    finally:
        root_logger.setLevel(original_root_level)

    assert "Fetched (403)" in capture.text()
    assert scrapling_logger.level == original_scrapling_level
    assert scrapling_logger.propagate == original_scrapling_propagate

    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "extractorError": capture.text(),
        }
    )

    assert classification == {
        "failureReason": "forbidden_403",
        "recommendedStrategy": "manual_review_or_proxy_required",
        "severity": "error",
    }


def test_classify_http_status_403_without_log_text() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {"status": "empty", "valid": 0, "extracted": 0, "httpStatus": 403}
    )

    assert classification["failureReason"] == "forbidden_403"
    assert classification["recommendedStrategy"] == "manual_review_or_proxy_required"
    assert classification["severity"] == "error"


def test_classify_anti_bot_challenge_from_extractor_error() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "extractorError": (
                "anti_bot_access_denied: "
                "sec-if-cpt-container Powered and protected by Akamai"
            ),
        }
    )

    assert classification == {
        "failureReason": "anti_bot_access_denied",
        "recommendedStrategy": "manual_review_or_proxy_required",
        "severity": "error",
    }


def test_classify_connection_closed_as_retryable_network_failure() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "extractorError": "Error: Page.goto: net::ERR_CONNECTION_CLOSED",
        }
    )

    assert classification["failureReason"] == "network_unavailable"
    assert classification["recommendedStrategy"] == "retry_network_or_proxy"
    assert classification["severity"] == "warning"


def test_classify_playwright_failed_load_as_retryable_network_failure() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "extractorError": (
                "RuntimeError: Failed to load "
                "'https://www.volkswagen.fi/fi/rakenna-auto.html' "
                "in Playwright: Page.goto: net::ERR_CONNECTION_CLOSED"
            ),
        }
    )

    assert classification["failureReason"] == "network_unavailable"
    assert classification["recommendedStrategy"] == "retry_network_or_proxy"
    assert classification["severity"] == "warning"


def test_classify_placeholder_source_url_as_source_repair() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "sourceUrl": "https://todo.invalid/be/bmw/x1",
            "extractorError": "DNSError: Could not resolve host: todo.invalid",
        }
    )

    assert classification == {
        "failureReason": "placeholder_source_url",
        "recommendedStrategy": "replace_placeholder_with_official_source",
        "severity": "error",
    }
    assert not batch_dryrun._source_result_is_retryable(
        {"status": "empty", "valid": 0, **classification},
        classification,
    )


def test_classify_missing_dynamic_price_as_retryable() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "extractorError": (
                "WARNING jato_scraper.extractors.playwright_card_flow — "
                "No plausible trim-overview MSRP appeared within 10000ms"
            ),
        }
    )

    assert classification["failureReason"] == "dynamic_price_not_ready"
    assert classification["recommendedStrategy"] == "retry_or_reduce_concurrency"
    assert classification["severity"] == "warning"
    assert batch_dryrun._source_result_is_retryable(
        {"status": "empty", "valid": 0, "failureReason": "dynamic_price_not_ready"},
        classification,
    )


def test_classify_discontinued_model_url_as_business_resolution() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "sourceUrl": "https://www.mercedes-benz.no/passengercars/models/suv/eqb/overview.html",
            "finalUrl": "https://www.mercedes-benz.no/our-brands/eqb-ikke-tilgjengelig/",
            "httpStatus": 200,
        }
    )

    assert classification == {
        "failureReason": "model_not_currently_available",
        "recommendedStrategy": "exclude_or_replace_discontinued_model",
        "severity": "info",
    }


def test_classify_model_page_redirected_to_homepage() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "sourceUrl": "https://www.toyota.no/nybil/yaris-cross",
            "finalUrl": "https://www.toyota.no/",
            "httpStatus": 200,
        }
    )

    assert classification == {
        "failureReason": "source_url_redirected_to_homepage",
        "recommendedStrategy": "update_source_url_or_confirm_model_availability",
        "severity": "warning",
    }


def test_classify_cross_market_redirect_as_environment_recheck() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "sourceUrl": "https://www.polestar.com/no/polestar-3/specifications/",
            "finalUrl": "https://www.polestar.cn/no/polestar-3/specifications/",
            "httpStatus": 404,
        }
    )

    assert classification == {
        "failureReason": "geo_market_redirect",
        "recommendedStrategy": "run_with_target_market_egress_or_official_snapshot",
        "severity": "warning",
    }
    assert not batch_dryrun._source_result_is_retryable(
        {"status": "empty", "valid": 0, **classification}, classification
    )


def test_classify_browser_runtime_failure_as_retryable() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "extractorError": (
                "TargetClosedError: BrowserType.launch_persistent_context: "
                "Target page, context or browser has been closed; signal=SIGABRT"
            ),
        }
    )

    assert classification == {
        "failureReason": "browser_runtime_unavailable",
        "recommendedStrategy": "restart_playwright_runtime_and_recheck",
        "severity": "warning",
    }
    assert batch_dryrun._source_result_is_retryable(
        {"status": "empty", "valid": 0, **classification}, classification
    )


def test_source_result_retryable_for_timeout_only() -> None:
    assert batch_dryrun._source_result_is_retryable(
        {"status": "empty", "valid": 0, "failureReason": "http_timeout"},
        {"failureReason": "http_timeout"},
    )
    assert not batch_dryrun._source_result_is_retryable(
        {"status": "empty", "valid": 0, "failureReason": "forbidden_403"},
        {"failureReason": "forbidden_403"},
    )
    assert not batch_dryrun._source_result_is_retryable(
        {"status": "dry_run", "valid": 2, "failureReason": None},
        {"failureReason": None},
    )


def test_classify_price_floor_rejection_as_price_out_of_range() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "dry_run",
            "valid": 0,
            "extracted": 1,
            "rejected": 1,
            "rejectedReasons": ["msrp_value=229.0 < 5000.0 for base_msrp"],
            "rejectionRuleCounts": {"price_range": 1},
        }
    )

    assert classification == {
        "failureReason": "price_out_of_range",
        "recommendedStrategy": "check_currency_and_price_semantics",
        "severity": "warning",
    }


def test_copy_source_result_diagnostics_preserves_rejection_details() -> None:
    src = {
        "sourceUrl": "https://www.nissan.fr/vehicules/neufs/qashqai.html",
        "rejectedReasons": ["msrp_value=229.0 < 5000.0 for base_msrp"],
        "rejectedRules": ["price_range"],
        "rejectionReasonCounts": {
            "msrp_value=229.0 < 5000.0 for base_msrp": 1,
        },
        "rejectionRuleCounts": {"price_range": 1},
        "sampleRejectedObservations": [
            {
                "officialModel": "QASHQAI",
                "officialTrim": "Personnalisation et style",
                "msrpValue": 229,
            },
        ],
        "unrelatedRuntimeKey": "not copied",
    }
    result_entry = {"code": "nissan_qashqai_fr_draft_scrapling"}

    batch_dryrun._copy_source_result_diagnostics(src, result_entry)

    assert result_entry["sourceUrl"] == src["sourceUrl"]
    assert result_entry["rejectedReasons"] == src["rejectedReasons"]
    assert result_entry["rejectedRules"] == ["price_range"]
    assert result_entry["rejectionRuleCounts"] == {"price_range": 1}
    assert result_entry["sampleRejectedObservations"] == src["sampleRejectedObservations"]
    assert "unrelatedRuntimeKey" not in result_entry


def test_parse_dryrun_args_supports_repeated_source_code_filter() -> None:
    batch, source_codes = batch_dryrun._parse_dryrun_args([
        "all",
        "--source-code",
        "renault_austral_es_draft_scrapling",
        "--source-code=renault_symbioz_nl_draft_scrapling",
    ])

    assert batch == "all"
    assert source_codes == [
        "renault_austral_es_draft_scrapling",
        "renault_symbioz_nl_draft_scrapling",
    ]


def test_parse_dryrun_args_defaults_batch_to_all_for_source_filter() -> None:
    batch, source_codes = batch_dryrun._parse_dryrun_args([
        "--source-code",
        "renault_austral_es_draft_scrapling",
    ])

    assert batch == "all"
    assert source_codes == ["renault_austral_es_draft_scrapling"]


def test_countries_for_all_batch_discovers_all_draft_country_dirs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    drafts = tmp_path / "drafts"
    for name in ["se", "cz", "it", "_shared", "readme", "nordic"]:
        (drafts / name).mkdir(parents=True)
    monkeypatch.setattr(batch_dryrun, "_DRAFTS_DIR", drafts)

    assert batch_dryrun._countries_for_batch("all") == ["cz", "it", "se"]


def test_countries_for_named_batches_and_explicit_list() -> None:
    assert batch_dryrun._countries_for_batch("1") == ["se", "hr"]
    assert batch_dryrun._countries_for_batch("2") == ["hu", "no", "at", "cz", "ch"]
    assert batch_dryrun._countries_for_batch("se,fi,dk") == ["se", "fi", "dk"]


def test_select_target_codes_filters_requested_drafts_and_promoted_sources() -> None:
    target_codes, skipped_promoted, missing_requested = (
        batch_dryrun._select_target_codes(
            draft_codes=[
                "renault_austral_es_draft_scrapling",
                "renault_symbioz_nl_draft_scrapling",
                "skoda_kamiq_cz_draft_scrapling",
            ],
            promoted_codes={"renault_symbioz_nl_scrapling"},
            countries=["es", "nl", "cz"],
            requested_source_codes=[
                "renault_austral_es_draft_scrapling",
                "renault_symbioz_nl_draft_scrapling",
                "missing_draft_scrapling",
            ],
        )
    )

    assert target_codes == [("es", "renault_austral_es_draft_scrapling")]
    assert skipped_promoted == [
        ("renault_symbioz_nl_draft_scrapling", "renault_symbioz_nl_scrapling")
    ]
    assert missing_requested == ["missing_draft_scrapling"]


def test_select_target_codes_source_filter_bypasses_batch_country_filter() -> None:
    target_codes, skipped_promoted, missing_requested = (
        batch_dryrun._select_target_codes(
            draft_codes=["renault_austral_es_draft_scrapling"],
            promoted_codes=set(),
            countries=["se", "no"],
            requested_source_codes=["renault_austral_es_draft_scrapling"],
        )
    )

    assert target_codes == [("es", "renault_austral_es_draft_scrapling")]
    assert skipped_promoted == []
    assert missing_requested == []


def test_source_attempt_limit_env(monkeypatch) -> None:
    monkeypatch.setenv("JATO_MSRP_DRYRUN_SOURCE_ATTEMPTS", "3")
    assert batch_dryrun._source_attempt_limit() == 3

    monkeypatch.setenv("JATO_MSRP_DRYRUN_SOURCE_ATTEMPTS", "bad")
    assert batch_dryrun._source_attempt_limit() == 2


def test_source_timeout_env_prefers_msrp_specific_value(monkeypatch) -> None:
    monkeypatch.setenv("JATO_SOURCE_TIMEOUT_SECONDS", "9")
    monkeypatch.setenv("JATO_MSRP_DRYRUN_SOURCE_TIMEOUT_SECONDS", "7")

    assert batch_dryrun._source_timeout_seconds() == 7


def test_source_attempt_timeout_defaults_to_source_timeout_plus_buffer(
    monkeypatch,
) -> None:
    monkeypatch.delenv("JATO_MSRP_DRYRUN_ATTEMPT_TIMEOUT_SECONDS", raising=False)

    assert batch_dryrun._source_attempt_timeout_seconds(10) == 70
    assert batch_dryrun._source_attempt_timeout_seconds(0) == 0


def test_configured_source_timeout_uses_profile_timeout(monkeypatch) -> None:
    class FakeRegistry:
        @staticmethod
        def get(code: str):
            assert code == "demo_source"
            return SimpleNamespace(profile=SimpleNamespace(timeout_seconds=300))

    monkeypatch.setitem(
        sys.modules,
        "jato_scraper",
        SimpleNamespace(registry=FakeRegistry),
    )

    assert batch_dryrun._configured_source_timeout_seconds("demo_source", 180) == 330


def test_classify_hard_attempt_timeout() -> None:
    exc = batch_dryrun.SourceAttemptTimeoutError(
        "source demo exceeded 1s run_scrape attempt timeout"
    )

    classification = batch_dryrun._classify_dryrun_failure({}, exception=exc)

    assert classification["failureReason"] == "http_timeout"
    assert classification["recommendedStrategy"] == "retry_or_reduce_concurrency"


def test_run_scrape_once_passes_source_timeout_and_captures_logs() -> None:
    calls = []

    def fake_run_scrape(*, source_codes, dry_run, source_timeout_seconds):
        calls.append({
            "source_codes": source_codes,
            "dry_run": dry_run,
            "source_timeout_seconds": source_timeout_seconds,
        })
        logging.getLogger("jato_scraper.demo").info("demo extractor diagnostic")
        return {
            "sources": {
                "demo_source": {
                    "status": "dry_run",
                    "valid": 1,
                    "extracted": 1,
                    "rejected": 0,
                }
            }
        }

    summary, captured_log_text = batch_dryrun._run_scrape_once(
        fake_run_scrape,
        "demo_source",
        source_timeout_seconds=11,
    )

    assert calls == [{
        "source_codes": ["demo_source"],
        "dry_run": True,
        "source_timeout_seconds": 11,
    }]
    assert summary["sources"]["demo_source"]["valid"] == 1
    assert "demo extractor diagnostic" in captured_log_text


def test_run_scrape_attempt_hard_timeout_stops_hung_attempt() -> None:
    if "fork" not in batch_dryrun.multiprocessing.get_all_start_methods():
        pytest.skip("hard attempt timeout requires fork start method")

    started = time.monotonic()

    with pytest.raises(batch_dryrun.SourceAttemptTimeoutError) as excinfo:
        batch_dryrun._run_scrape_attempt(
            _slow_run_scrape,
            "slow_source",
            source_timeout_seconds=30,
            attempt_timeout_seconds=1,
        )

    assert "slow_source" in str(excinfo.value)
    assert time.monotonic() - started < 4


def test_write_source_repair_backlog_artifact_reuses_aggregate_writer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls = []

    def fake_writer(report: dict, report_dir: Path) -> None:
        calls.append((report, report_dir))
        (report_dir / "msrp_source_repair_backlog.json").write_text(
            "{}",
            encoding="utf-8",
        )

    monkeypatch.setattr(batch_dryrun, "_write_v3_source_repair_backlog", fake_writer)
    report = {"schemaVersion": "msrp_dryrun_report_v3", "runId": "demo"}

    batch_dryrun._write_source_repair_backlog_artifact(report, tmp_path)

    assert calls == [(report, tmp_path)]
    assert (tmp_path / "msrp_source_repair_backlog.json").is_file()


def test_write_dryrun_report_artifacts_preserves_latest_for_source_filter(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls = []

    def fake_writer(report: dict, report_dir: Path) -> None:
        calls.append((report, report_dir))

    monkeypatch.setattr(batch_dryrun, "_write_v3_source_repair_backlog", fake_writer)
    latest_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-stable",
        "summary": {"passPct": 96.8},
    }
    (tmp_path / "dryrun_report.json").write_text(
        json.dumps(latest_report),
        encoding="utf-8",
    )
    (tmp_path / "dryrun_runs_index.json").write_text(
        json.dumps({
            "schemaVersion": "msrp_dryrun_runs_index_v1",
            "latestRunId": "msrp-dryrun-stable",
            "runs": [{
                "runId": "msrp-dryrun-stable",
                "artifactPath": "03_Scripts/diagnostics/artifacts/dryrun_report_msrp-dryrun-stable.json",
            }],
        }),
        encoding="utf-8",
    )
    diagnostic_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-source-diagnostic",
        "batch": "at",
        "isSourceFiltered": True,
        "sourceFilter": ["mazda_cx_30_at_draft_scrapling"],
        "expectedCountries": ["at"],
        "observedCountries": ["at"],
        "missingCountries": [],
        "summary": {
            "status": "success",
            "gateStatus": "allowed",
            "gateThreshold": 70,
            "passPct": 100.0,
            "total": 1,
            "pass": 1,
            "empty": 0,
            "fail": 0,
            "errors": 0,
        },
    }

    latest_path, history_path = batch_dryrun._write_dryrun_report_artifacts(
        diagnostic_report,
        tmp_path,
        update_latest=False,
    )

    assert latest_path == tmp_path / "dryrun_report.json"
    assert history_path.is_file()
    assert json.loads(latest_path.read_text()) == latest_report
    index = json.loads((tmp_path / "dryrun_runs_index.json").read_text())
    assert index["latestRunId"] == "msrp-dryrun-stable"
    assert index["runs"][0]["runId"] == "msrp-dryrun-source-diagnostic"
    assert index["runs"][0]["updatesLatestArtifact"] is False
    assert index["runs"][0]["isSourceFiltered"] is True
    assert index["runs"][0]["sourceFilter"] == ["mazda_cx_30_at_draft_scrapling"]
    assert calls == []


def test_source_probe_rebuilds_missing_stable_index_from_latest_artifact(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(batch_dryrun, "_write_v3_source_repair_backlog", lambda *_: None)
    stable_report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-stable",
        "batch": "se,fi",
        "isSourceFiltered": False,
        "sourceFilter": [],
        "expectedCountries": ["se", "fi"],
        "observedCountries": ["se", "fi"],
        "missingCountries": [],
        "summary": {
            "status": "success",
            "gateStatus": "allowed",
            "gateThreshold": 70,
            "passPct": 96.8,
            "total": 31,
            "pass": 30,
            "empty": 1,
            "fail": 0,
            "errors": 0,
        },
    }
    diagnostic_report = {
        **stable_report,
        "runId": "msrp-dryrun-source-diagnostic",
        "isSourceFiltered": True,
        "sourceFilter": ["polestar_4_no_draft_scrapling"],
        "expectedCountries": ["no"],
        "observedCountries": ["no"],
        "summary": {**stable_report["summary"], "total": 1, "pass": 1, "passPct": 100.0},
    }
    (tmp_path / "dryrun_report.json").write_text(json.dumps(stable_report), encoding="utf-8")
    (tmp_path / "dryrun_runs_index.json").write_text(
        json.dumps({"schemaVersion": "msrp_dryrun_runs_index_v1", "latestRunId": None, "runs": []}),
        encoding="utf-8",
    )

    batch_dryrun._write_dryrun_report_artifacts(
        diagnostic_report,
        tmp_path,
        update_latest=False,
    )

    index = json.loads((tmp_path / "dryrun_runs_index.json").read_text())
    assert index["latestRunId"] == "msrp-dryrun-stable"
    assert [run["runId"] for run in index["runs"]] == [
        "msrp-dryrun-source-diagnostic",
        "msrp-dryrun-stable",
    ]
    assert index["runs"][1]["updatesLatestArtifact"] is True
    assert index["runs"][1]["isSourceFiltered"] is False


def test_write_dryrun_report_artifacts_updates_latest_for_full_run(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls = []

    def fake_writer(report: dict, report_dir: Path) -> None:
        calls.append((report, report_dir))

    monkeypatch.setattr(batch_dryrun, "_write_v3_source_repair_backlog", fake_writer)
    report = {
        "schemaVersion": "msrp_dryrun_report_v3",
        "runId": "msrp-dryrun-full",
        "batch": "at",
        "isSourceFiltered": False,
        "sourceFilter": [],
        "expectedCountries": ["at"],
        "observedCountries": ["at"],
        "missingCountries": [],
        "summary": {
            "status": "success",
            "gateStatus": "allowed",
            "gateThreshold": 70,
            "passPct": 96.8,
            "total": 31,
            "pass": 30,
            "empty": 1,
            "fail": 0,
            "errors": 0,
        },
    }

    latest_path, history_path = batch_dryrun._write_dryrun_report_artifacts(
        report,
        tmp_path,
        update_latest=True,
    )

    assert json.loads(latest_path.read_text())["runId"] == "msrp-dryrun-full"
    assert json.loads(history_path.read_text())["runId"] == "msrp-dryrun-full"
    index = json.loads((tmp_path / "dryrun_runs_index.json").read_text())
    assert index["latestRunId"] == "msrp-dryrun-full"
    assert index["runs"][0]["updatesLatestArtifact"] is True
    assert index["runs"][0]["isSourceFiltered"] is False
    assert calls == [(report, tmp_path)]


def test_write_dryrun_status_best_effort_does_not_block_report_generation(
    monkeypatch,
    caplog,
) -> None:
    def failing_status_writer(*args, **kwargs) -> None:
        raise PermissionError("status file locked")

    monkeypatch.setattr(batch_dryrun, "_write_dryrun_status", failing_status_writer)

    with caplog.at_level(logging.WARNING):
        batch_dryrun._write_dryrun_status_best_effort(
            ["fr"],
            pass_count=0,
            empty_count=0,
            fail_count=0,
            error_count=1,
            total=1,
            results=[],
        )

    assert "continuing report generation" in caplog.text
