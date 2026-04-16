from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = PROJECT_ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services import local_wiki_service  # noqa: E402

PROCESSED_DIR = PROJECT_ROOT / "04_Processed_data"


def find_source_dataset() -> Path:
    preferred_files = [
        PROCESSED_DIR / "jato_full_archive.parquet",
        PROCESSED_DIR / "partitioned_dataset_v1",
    ]
    for candidate in preferred_files:
        if candidate.exists():
            return candidate

    parquet_files = list(PROCESSED_DIR.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError("No parquet files found in 04_Processed_data/")
    return max(parquet_files, key=os.path.getmtime)


def main() -> None:
    parquet_path = find_source_dataset()
    print(f"Reading dataset: {parquet_path}")
    frame = pd.read_parquet(parquet_path)

    print("Building offline vehicle_wiki collection...")
    manifest = local_wiki_service.build_vehicle_wiki_from_dataframe(
        frame,
        source_path=parquet_path,
    )

    print("vehicle_wiki build completed.")
    print(f"  collection: {manifest['collectionName']}")
    print(f"  documents : {manifest['documentCount']}")
    print(f"  db path   : {manifest['dbPath']}")
    print(
        "  manifest  : "
        f"{local_wiki_service.get_local_wiki_manifest_path()}"
    )


if __name__ == "__main__":
    main()
