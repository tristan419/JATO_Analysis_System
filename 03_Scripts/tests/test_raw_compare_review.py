from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "raw_compare_review.py"


def load_module():
    module_name = "raw_compare_review_test_module"
    if module_name in sys.modules:
        return sys.modules[module_name]

    script_root = str(MODULE_PATH.parent)
    if script_root not in sys.path:
        sys.path.insert(0, script_root)

    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


raw_compare_module = load_module()


def test_compute_payload_digest_ignores_row_order() -> None:
    original = pd.DataFrame(
        {
            "trim": ["Life", "Style", "Life"],
            "msrp": [100.0, 200.0, 100.0],
            "note": [None, " keep ", None],
        }
    )
    reordered = original.iloc[[2, 0, 1]].reset_index(drop=True)

    assert raw_compare_module.compute_payload_digest(
        original
    ) == raw_compare_module.compute_payload_digest(reordered)


def test_build_key_digest_frame_preserves_group_digests_when_rows_reordered(
) -> None:
    key_groups = [
        {"id": "country", "oldColumn": "国家", "newColumn": "国家"},
        {"id": "model", "oldColumn": "model", "newColumn": "model"},
    ]
    base = pd.DataFrame(
        {
            "国家": ["SE", "SE", "SE", "DE"],
            "model": ["EX30", "EX30", "XC60", "Q4"],
            "trim": ["Plus", "Plus", "Ultra", "Edition"],
            "price": [1, 1, 2, 3],
        }
    )
    reordered = base.iloc[[2, 0, 3, 1]].reset_index(drop=True)

    actual = raw_compare_module.build_key_digest_frame(
        base,
        key_groups,
        "old",
        ["trim", "price"],
    ).sort_values(["country", "model"]).reset_index(drop=True)
    reordered_actual = raw_compare_module.build_key_digest_frame(
        reordered,
        key_groups,
        "old",
        ["trim", "price"],
    ).sort_values(["country", "model"]).reset_index(drop=True)

    assert actual[["country", "model", "payloadDigest"]].to_dict(
        "records"
    ) == reordered_actual[["country", "model", "payloadDigest"]].to_dict(
        "records"
    )
    assert actual[["country", "model", "recordCount", "multiRow"]].to_dict(
        "records"
    ) == [
        {
            "country": "DE",
            "model": "Q4",
            "recordCount": 1,
            "multiRow": False,
        },
        {
            "country": "SE",
            "model": "EX30",
            "recordCount": 2,
            "multiRow": True,
        },
        {
            "country": "SE",
            "model": "XC60",
            "recordCount": 1,
            "multiRow": False,
        },
    ]
