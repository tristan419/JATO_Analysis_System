from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "elt_worker.py"


def load_module():
    module_name = "elt_worker_test_module"
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


elt_worker_module = load_module()


def test_supplement_missing_countries_from_parquet_adds_absent_country_rows(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "patch.xlsx"
    source_path.write_bytes(b"fake")
    current_df = elt_worker_module.add_source_tracking_columns(
        pd.DataFrame(
            [
                {"国家": "德国", "Model": "ID.4", "2026 Mar": 21},
            ]
        ),
        source_file=source_path,
        source_index=1,
    )
    current_df = elt_worker_module.normalize_dataframe(current_df)

    supplement_path = tmp_path / "active.parquet"
    pd.DataFrame(
        [
            {"国家": "德国", "Model": "ID.4", "2026 Mar": 21},
            {"国家": "瑞典", "Model": "EX30", "2026 Mar": 12},
        ]
    ).to_parquet(supplement_path, index=False)

    merged_df, summary = elt_worker_module.supplement_missing_countries_from_parquet(
        current_df,
        str(supplement_path),
        source_index=2,
    )

    assert summary["enabled"] is True
    assert summary["supplementedCountryCount"] == 1
    assert summary["supplementedCountries"] == ["瑞典"]
    assert summary["supplementedRowCount"] == 1
    assert sorted(merged_df["国家"].astype("string").tolist()) == ["德国", "瑞典"]
