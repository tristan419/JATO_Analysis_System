from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


SCRIPTS_ROOT = Path(__file__).resolve().parents[1]
PARTITION_SCRIPT = (
    SCRIPTS_ROOT / "data_pipeline" / "build_partitioned_dataset.py"
)
PARTITION_SPEC = importlib.util.spec_from_file_location(
    "jato_streaming_partition_equivalence",
    PARTITION_SCRIPT,
)
assert PARTITION_SPEC is not None and PARTITION_SPEC.loader is not None
PARTITION = importlib.util.module_from_spec(PARTITION_SPEC)
PARTITION_SPEC.loader.exec_module(PARTITION)

SUMMARY_SCRIPT = SCRIPTS_ROOT / "data_pipeline" / "precompute_summaries.py"
SUMMARY_SPEC = importlib.util.spec_from_file_location(
    "jato_streaming_summary_equivalence",
    SUMMARY_SCRIPT,
)
assert SUMMARY_SPEC is not None and SUMMARY_SPEC.loader is not None
SUMMARY = importlib.util.module_from_spec(SUMMARY_SPEC)
SUMMARY_SPEC.loader.exec_module(SUMMARY)


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_streaming_partition_manifest_and_signatures_match_legacy_small_sample(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "candidate.parquet"
    source = pd.DataFrame(
        [
            {"国家": "捷克", "Make": "SKODA", "RowId": 1, "2026 Jun": 11},
            {"国家": "丹麦", "Make": "VOLVO", "RowId": 2, "2026 Jun": 12},
            {"国家": "捷克", "Make": "SKODA", "RowId": 3, "2026 Jun": 13},
            {"国家": "德国", "Make": "BMW", "RowId": 4, "2026 Jun": 14},
            {"国家": "丹麦", "Make": "VOLVO", "RowId": 5, "2026 Jun": 15},
        ]
    )
    source.to_parquet(source_path, index=False, row_group_size=2)
    legacy_root = tmp_path / "legacy"
    streaming_root = tmp_path / "streaming"

    _legacy_dir, legacy_manifest_path = PARTITION.build_partitioned_dataset(
        input_path=str(source_path),
        output_dir=str(legacy_root),
        partition_cols=["国家"],
        overwrite=True,
        incremental=False,
        job_id="legacy-small",
    )
    _streaming_dir, streaming_manifest_path = (
        PARTITION.build_partitioned_dataset_streaming(
            input_path=str(source_path),
            output_dir=str(streaming_root),
            partition_cols=["国家"],
            overwrite=True,
            job_id="streaming-small",
            batch_size=2,
        )
    )

    legacy = _read_json(legacy_manifest_path)
    streaming = _read_json(streaming_manifest_path)
    for key in (
        "manifestSchemaVersion",
        "sourceManifestSchemaVersion",
        "partitionColumns",
        "rows",
        "columns",
        "partitionDirectoryCount",
        "partitionDirectories",
        "partitionStats",
    ):
        assert streaming[key] == legacy[key]
    assert streaming["validationSummary"]["inputRows"] == legacy[
        "validationSummary"
    ]["inputRows"]
    assert streaming["validationSummary"]["inputColumns"] == legacy[
        "validationSummary"
    ]["inputColumns"]
    assert streaming["validationSummary"]["partitionCardinality"] == legacy[
        "validationSummary"
    ]["partitionCardinality"]
    assert streaming["validationSummary"]["streaming"] == {
        "batchRows": 2,
        "maxMaterializedRows": 2,
    }
    for key in (
        "addedPartitions",
        "updatedPartitions",
        "removedPartitions",
        "rewrittenPartitions",
    ):
        assert streaming["partitionUpdateSummary"][key] == legacy[
            "partitionUpdateSummary"
        ][key]

    for partition_dir in legacy["partitionDirectories"]:
        legacy_frame = pd.read_parquet(legacy_root / partition_dir)
        streaming_frame = pd.read_parquet(streaming_root / partition_dir)
        pd.testing.assert_frame_equal(
            legacy_frame.sort_values("RowId").reset_index(drop=True),
            streaming_frame.sort_values("RowId").reset_index(drop=True),
        )


def test_bounded_full_summaries_match_legacy_small_sample(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "candidate.parquet"
    source = pd.DataFrame(
        [
            {
                "国家": "捷克",
                "Powertrain": "BEV",
                "Segment": "C",
                "Metric": 100.0,
                "2026 May": 10,
                "2026 Jun": 11,
            },
            {
                "国家": "捷克",
                "Powertrain": "ICE",
                "Segment": "D",
                "Metric": 200.0,
                "2026 May": 20,
                "2026 Jun": 21,
            },
            {
                "国家": "丹麦",
                "Powertrain": "BEV",
                "Segment": "C",
                "Metric": 300.0,
                "2026 May": 30,
                "2026 Jun": 31,
            },
            {
                "国家": "德国",
                "Powertrain": "PHEV",
                "Segment": "E",
                "Metric": 400.0,
                "2026 May": 40,
                "2026 Jun": 41,
            },
        ]
    )
    source.to_parquet(source_path, index=False, row_group_size=2)
    expected = {
        "country": SUMMARY.compute_country_summary(source),
        "yearMonth": SUMMARY.compute_year_month_summary(source),
        "powertrain": SUMMARY.compute_powertrain_summary(source),
        "segment": SUMMARY.compute_segment_summary(source),
        "topMakes": SUMMARY.compute_top_makes_summary(source),
    }

    actual = SUMMARY.compute_all_summaries_bounded(
        str(source_path),
        scratch_parent=tmp_path / "scratch",
    )

    assert set(actual) == set(expected)
    for summary_name in expected:
        pd.testing.assert_frame_equal(
            actual[summary_name].reset_index(drop=True),
            expected[summary_name].reset_index(drop=True),
        )
    assert list((tmp_path / "scratch").glob(".jato-summary-duckdb-*")) == []


def test_streaming_part_files_preserve_source_schema_across_null_batches(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "typed-candidate.parquet"
    schema = pa.schema(
        [
            pa.field("国家", pa.string()),
            pa.field("Make", pa.string()),
            pa.field("Metric", pa.int64()),
        ]
    )
    with pq.ParquetWriter(source_path, schema) as writer:
        writer.write_table(
            pa.Table.from_pydict(
                {"国家": ["德国"], "Make": ["BMW"], "Metric": [None]},
                schema=schema,
            )
        )
        writer.write_table(
            pa.Table.from_pydict(
                {"国家": ["德国"], "Make": ["BMW"], "Metric": [5]},
                schema=schema,
            )
        )

    output_root = tmp_path / "streaming-typed"
    PARTITION.build_partitioned_dataset_streaming(
        input_path=str(source_path),
        output_dir=str(output_root),
        partition_cols=["国家"],
        overwrite=True,
        job_id="streaming-typed",
        batch_size=1,
    )

    part_files = sorted(output_root.rglob("part-*.parquet"))
    assert len(part_files) == 2
    assert all(
        pq.read_schema(path).field("Metric").type == pa.int64()
        for path in part_files
    )
