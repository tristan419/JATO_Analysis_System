from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "data_pipeline"
    / "rebuild_from_parquet.py"
)
SPEC = importlib.util.spec_from_file_location(
    "rebuild_from_parquet",
    SCRIPT_PATH,
)
assert SPEC is not None and SPEC.loader is not None
REBUILD = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REBUILD)


def test_rebuild_never_writes_canonical_summaries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    input_path = tmp_path / "candidate.parquet"
    output_dir = tmp_path / "staging" / "rebuild"
    partition_output = tmp_path / "staging" / "partitioned_dataset_v1"
    manifest_path = tmp_path / "staging" / "manifest.json"
    fingerprint_path = tmp_path / "staging" / "dataset_fingerprint.json"
    pd.DataFrame(
        {
            "国家": ["捷克", "丹麦"],
            "Make": ["SKODA", "VOLVO"],
            "2026 Jun": [1, 2],
        }
    ).to_parquet(input_path, index=False)
    monkeypatch.setattr(REBUILD, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "--input-parquet",
            str(input_path),
            "--output-dir",
            str(output_dir),
            "--partition-output",
            str(partition_output),
            "--manifest",
            str(manifest_path),
            "--fingerprint",
            str(fingerprint_path),
        ],
    )

    REBUILD.main()

    assert manifest_path.exists()
    assert fingerprint_path.exists()
    assert partition_output.exists()
    assert not (tmp_path / "04_Processed_data" / "summaries").exists()
