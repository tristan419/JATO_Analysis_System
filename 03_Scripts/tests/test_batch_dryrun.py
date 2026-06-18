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
