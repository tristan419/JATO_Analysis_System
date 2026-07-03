from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
import sys
import time

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


def test_classify_pdf_download_name_resolution_as_dns_failure() -> None:
    classification = batch_dryrun._classify_dryrun_failure(
        {
            "status": "empty",
            "valid": 0,
            "extracted": 0,
            "extractorError": (
                "pdf_direct_download_failed: HTTPSConnectionPool("
                "host='www.suzuki.cz', port=443): Max retries exceeded "
                "(Caused by NameResolutionError(\"Failed to resolve "
                "'www.suzuki.cz'\"))"
            ),
        }
    )

    assert classification["failureReason"] == "dns_resolution_failed"
    assert classification["recommendedStrategy"] == "retry_or_check_dns"
    assert classification["severity"] == "warning"


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
