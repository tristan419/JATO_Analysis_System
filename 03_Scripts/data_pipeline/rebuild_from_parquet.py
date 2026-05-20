"""Rebuild partition/manifest/fingerprint from an existing parquet file.

Used by Smart Merge to refresh derived artifacts after merging regressed countries
from the active dataset into the staging candidate. Avoids re-running the full
xlsx→parquet ETL.

Usage:
    python rebuild_from_parquet.py \\
        --input-parquet path/to/merged.parquet \\
        --output-dir path/to/staging \\
        --partition-output path/to/partitioned_dataset_v1 \\
        --manifest path/to/manifest.json \\
        --fingerprint path/to/dataset_fingerprint.json
"""

import argparse
import hashlib
import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

SCRIPTS_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from data_pipeline.build_partitioned_dataset import build_partitioned_dataset
from logging_utils import build_job_id


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild partition/manifest/fingerprint from an existing parquet."
    )
    parser.add_argument("--input-parquet", required=True, help="Path to the merged parquet")
    parser.add_argument("--output-dir", required=True, help="Staging output directory")
    parser.add_argument("--partition-output", required=True, help="Partition output directory")
    parser.add_argument("--manifest", required=True, help="Manifest JSON path")
    parser.add_argument("--fingerprint", required=True, help="Fingerprint JSON path")
    args = parser.parse_args()

    input_path = resolve_path(args.input_parquet)
    output_dir = resolve_path(args.output_dir)
    partition_output = resolve_path(args.partition_output)
    manifest_path = resolve_path(args.manifest)
    fingerprint_path = resolve_path(args.fingerprint)

    if not input_path.exists():
        print(f"ERROR: input parquet not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    job_id = build_job_id()
    steps: dict[str, float] = {}

    # 1. Build partitioned dataset
    t0 = time.time()
    if partition_output.exists():
        shutil.rmtree(partition_output)
    partition_dir, partition_manifest = build_partitioned_dataset(
        input_path=str(input_path),
        output_dir=str(partition_output),
        overwrite=True,
        incremental=False,
        job_id=job_id,
    )
    steps["partitionSeconds"] = round(time.time() - t0, 3)
    print(f"[ok] partitioned dataset → {partition_dir}")

    # 2. Build manifest
    t0 = time.time()
    pf = pq.ParquetFile(input_path)
    row_count = pf.metadata.num_rows
    col_count = pf.metadata.num_columns
    file_size = input_path.stat().st_size
    file_hash = hashlib.sha256(input_path.read_bytes()).hexdigest()

    manifest = {
        "schemaVersion": "1.0",
        "builtAt": datetime.now(timezone.utc).isoformat(),
        "jobId": job_id,
        "rows": row_count,
        "columns": col_count,
        "fileSizeBytes": file_size,
        "sha256": file_hash,
        "parquetFileCount": 1,
        "parquetFiles": [str(input_path.name)],
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    steps["manifestSeconds"] = round(time.time() - t0, 3)
    print(f"[ok] manifest → {manifest_path}")

    # 3. Build fingerprint
    fingerprint = {
        "schemaVersion": "1.0",
        "builtAt": datetime.now(timezone.utc).isoformat(),
        "sha256": file_hash,
        "rowCount": row_count,
        "columnCount": col_count,
    }
    fingerprint_path.parent.mkdir(parents=True, exist_ok=True)
    fingerprint_path.write_text(
        json.dumps(fingerprint, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    steps["fingerprintSeconds"] = round(time.time() - t0, 3)
    print(f"[ok] fingerprint → {fingerprint_path}")

    # 4. Precompute summaries (best-effort)
    try:
        t0 = time.time()
        from precompute_summaries import precompute_all_summaries  # type: ignore

        summaries_output = PROJECT_ROOT / "04_Processed_data" / "summaries"
        precompute_all_summaries(
            parquet_path=str(input_path),
            output_dir=str(summaries_output),
            partitioned_dataset_path=str(partition_output),
        )
        steps["precomputeSeconds"] = round(time.time() - t0, 3)
        print(f"[ok] precomputed summaries → {summaries_output}")
    except Exception as exc:
        steps["precomputeSeconds"] = -1.0
        print(f"[warn] precompute_summaries skipped: {exc}")

    report = {
        "jobStatus": "success",
        "jobElapsedSeconds": round(sum(v for v in steps.values() if v > 0), 3),
        "steps": steps,
        "rowCount": row_count,
        "partitionDir": str(partition_dir),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "rebuild_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[ok] rebuild complete: {row_count} rows, {sum(steps.values()):.1f}s total")


if __name__ == "__main__":
    main()
