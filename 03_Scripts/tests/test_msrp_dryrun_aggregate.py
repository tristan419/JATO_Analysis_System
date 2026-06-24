from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "msrp_dryrun_aggregate.py"


def load_module():
    module_name = "msrp_dryrun_aggregate_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


aggregate = load_module()


def test_normalize_source_result_preserves_rejection_diagnostics() -> None:
    result = {
        "country": "fr",
        "code": "nissan_qashqai_fr_draft_scrapling",
        "status": "dry_run",
        "valid": 0,
        "extracted": 1,
        "rejected": 1,
        "failureReason": "price_out_of_range",
        "recommendedStrategy": "check_currency_and_price_semantics",
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
    }

    normalized = aggregate._normalize_source_result(result, "fr")

    assert normalized["status"] == "fail"
    assert normalized["failureReason"] == "price_out_of_range"
    assert normalized["rejectedReasons"] == result["rejectedReasons"]
    assert normalized["rejectedRules"] == ["price_range"]
    assert normalized["rejectionReasonCounts"] == result["rejectionReasonCounts"]
    assert normalized["rejectionRuleCounts"] == {"price_range": 1}
    assert (
        normalized["sampleRejectedObservations"]
        == result["sampleRejectedObservations"]
    )
