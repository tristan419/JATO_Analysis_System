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


def test_allow_missing_countries_treats_absent_baseline_scope_as_unchanged(
) -> None:
    old_info = {
        "德国": {"latestMonth": "2026 Jan", "rowCount": 10, "months": ["2026 Jan"]},
        "瑞典": {"latestMonth": "2026 Jan", "rowCount": 12, "months": ["2026 Jan"]},
    }
    new_info = {
        "德国": {
            "latestMonth": "2026 Mar",
            "rowCount": 15,
            "months": ["2026 Jan", "2026 Feb", "2026 Mar"],
        }
    }

    freshness = raw_compare_module.summarize_country_freshness(
        old_info,
        new_info,
        allow_missing_countries=True,
    )
    coverage = raw_compare_module.summarize_country_coverage(
        old_info,
        new_info,
        allow_missing_countries=True,
    )
    scope = raw_compare_module.build_country_scope_summary(
        freshness,
        coverage,
        old_countries=["德国", "瑞典"],
        new_countries=["德国"],
        allow_missing_countries=True,
    )

    assert next(
        entry for entry in freshness if entry["country"] == "瑞典"
    ) == {
        "country": "瑞典",
        "oldLatestMonth": "2026 Jan",
        "newLatestMonth": "2026 Jan",
        "freshnessStatus": "unchanged_latest",
        "freshnessDeltaMonths": 0,
        "oldRowCount": 12,
        "newRowCount": 12,
        "rowDelta": 0,
    }
    assert next(
        entry for entry in coverage if entry["country"] == "瑞典"
    )["coverageStatus"] == "unchanged_coverage"
    assert scope["removedCountries"] == []
    assert scope["changedCountries"] == ["德国"]


def test_overlap_samples_are_collected_per_country() -> None:
    compare_plan = {
        "groups": [
            {"id": "country", "oldColumn": "国家", "newColumn": "国家"},
            {"id": "model", "oldColumn": "Model", "newColumn": "Model"},
        ],
        "compareKeyColumns": ["国家", "Model"],
    }
    old_df = pd.DataFrame(
        {
            "国家": ["A", "A", "B", "B"],
            "Model": ["m1", "m2", "m1", "m2"],
            "Trim": ["base", "plus", "base", "plus"],
            "2026 Jan": [10, 20, 30, 40],
        }
    )
    new_df = pd.DataFrame(
        {
            "国家": ["A", "A", "B", "B"],
            "Model": ["m1", "m2", "m1", "m2"],
            "Trim": ["base", "plus", "base", "plus"],
            "2026 Jan": [11, 21, 31, 41],
        }
    )
    coverage_entries = [
        {"country": "A", "overlappingMonths": ["2026 Jan"]},
        {"country": "B", "overlappingMonths": ["2026 Jan"]},
    ]

    _summaries, samples = raw_compare_module.summarize_overlap_changes(
        old_df=old_df,
        new_df=new_df,
        compare_plan=compare_plan,
        old_country_col="国家",
        new_country_col="国家",
        coverage_entries=coverage_entries,
        old_time_columns=["2026 Jan"],
        new_time_columns=["2026 Jan"],
        sample_limit=1,
    )

    assert len(samples) == 2
    assert [sample["country"] for sample in samples] == ["A", "B"]
