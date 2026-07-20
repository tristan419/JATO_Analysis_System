from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from urllib.parse import quote

import pandas as pd


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "data_pipeline"
    / "precompute_summaries.py"
)
SPEC = importlib.util.spec_from_file_location(
    "precompute_summaries",
    SCRIPT_PATH,
)
assert SPEC is not None and SPEC.loader is not None
PRECOMPUTE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PRECOMPUTE)


def _write_country_partition(
    partition_root: Path,
    country: str,
    rows: list[dict[str, object]],
) -> None:
    country_dir = partition_root / f"国家={quote(country, safe='')}"
    country_dir.mkdir(parents=True, exist_ok=True)
    # Production partition files omit 国家 because it is encoded in the path.
    pd.DataFrame(rows).to_parquet(
        country_dir / "part-00000.parquet",
        index=False,
    )


def test_incremental_summaries_keep_unchanged_country_in_fresh_temp_dir(
    tmp_path: Path,
) -> None:
    canonical_dir = tmp_path / "summaries"
    old_parquet = tmp_path / "old.parquet"
    pd.DataFrame(
        {
            "国家": ["捷克", "丹麦"],
            "Segment": ["C", "D"],
            "2026 May": [10, 20],
        }
    ).to_parquet(old_parquet, index=False)
    PRECOMPUTE.precompute_all_summaries(
        parquet_path=str(old_parquet),
        output_dir=str(canonical_dir),
    )
    canonical_country_before = pd.read_parquet(
        canonical_dir / "country_summary.parquet"
    )

    candidate_parquet = tmp_path / "candidate.parquet"
    candidate = pd.DataFrame(
        {
            "国家": ["捷克", "捷克", "丹麦"],
            "Segment": ["C", "C", "D"],
            "2026 May": [10, 5, 20],
        }
    )
    candidate.to_parquet(candidate_parquet, index=False)

    partition_root = tmp_path / "partitioned_dataset_v1"
    _write_country_partition(
        partition_root,
        "捷克",
        [
            {"Segment": "C", "2026 May": 10},
            {"Segment": "C", "2026 May": 5},
        ],
    )
    _write_country_partition(
        partition_root,
        "丹麦",
        [{"Segment": "D", "2026 May": 20}],
    )

    temp_dir = tmp_path / ".summaries.job.tmp"
    manifest = PRECOMPUTE.precompute_all_summaries(
        parquet_path=str(candidate_parquet),
        output_dir=str(temp_dir),
        partitioned_dataset_path=str(partition_root),
        changed_partition_keys=["捷克"],
        existing_country_summary_dir=str(canonical_dir),
    )

    country_summary = pd.read_parquet(
        temp_dir / "country_summary.parquet"
    ).set_index("国家")
    assert set(country_summary.index) == {"捷克", "丹麦"}
    assert int(country_summary.loc["捷克", "国家_count"]) == 2
    assert int(country_summary.loc["丹麦", "国家_count"]) == 1
    year_month_summary = pd.read_parquet(
        temp_dir / "yearMonth_summary.parquet"
    ).set_index("yearMonth")
    assert int(year_month_summary.loc["2026 May", "totalCount"]) == 35

    # Staging remains isolated: computing the candidate cannot alter canonical.
    pd.testing.assert_frame_equal(
        pd.read_parquet(canonical_dir / "country_summary.parquet"),
        canonical_country_before,
    )

    persisted_manifest = json.loads(
        (temp_dir / "summaries_manifest.json").read_text(encoding="utf-8")
    )
    assert persisted_manifest == manifest
    assert manifest["originalRowCount"] == len(candidate)
    assert manifest["summaries"]["country"]["rows"] == 2
    assert manifest["incremental"]["changedCountryKeys"] == ["捷克"]
    assert manifest["incremental"]["recomputedCountryKeys"] == ["捷克"]
    assert manifest["incremental"]["backfilledCountryKeys"] == []
    assert manifest["totalSummaryRows"] == sum(
        len(pd.read_parquet(item["parquet"]))
        for item in manifest["summaries"].values()
    )
