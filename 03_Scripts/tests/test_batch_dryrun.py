from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
import sys


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
