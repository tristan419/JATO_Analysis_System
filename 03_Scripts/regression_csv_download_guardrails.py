# pyright: reportMissingImports=false

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_ROOT = PROJECT_ROOT / "05_DashBoard"
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from dashboard.config import (  # noqa: E402
    CSV_DOWNLOAD_MAX_BYTES,
    CSV_DOWNLOAD_MAX_ROWS,
)
from dashboard.views import build_preview_csv_payload  # noqa: E402


def assert_case(
    name: str,
    preview_df: pd.DataFrame,
    expected_truncated: bool,
    expected_exceeds_size: bool,
) -> None:
    (
        _,
        csv_size_bytes,
        is_truncated,
        exceeds_size_limit,
    ) = build_preview_csv_payload(preview_df)
    print(
        f"{name}: rows={len(preview_df):,} size={csv_size_bytes:,} "
        f"truncated={is_truncated} exceeds={exceeds_size_limit}"
    )

    if is_truncated != expected_truncated:
        raise AssertionError(
            f"{name} truncated mismatch: "
            f"expected={expected_truncated} actual={is_truncated}"
        )

    if exceeds_size_limit != expected_exceeds_size:
        raise AssertionError(
            f"{name} size-limit mismatch: "
            f"expected={expected_exceeds_size} actual={exceeds_size_limit}"
        )


def main() -> None:
    print("CSV download guardrails regression")
    print(f"CSV_DOWNLOAD_MAX_ROWS={CSV_DOWNLOAD_MAX_ROWS:,}")
    print(f"CSV_DOWNLOAD_MAX_BYTES={CSV_DOWNLOAD_MAX_BYTES:,}")

    small_df = pd.DataFrame(
        {
            "country": ["DE", "FR", "ES"],
            "sales": [100, 120, 140],
        }
    )
    assert_case(
        "small",
        small_df,
        expected_truncated=False,
        expected_exceeds_size=False,
    )

    truncated_df = pd.DataFrame(
        {
            "country": ["DE"] * (CSV_DOWNLOAD_MAX_ROWS + 12),
            "sales": [1] * (CSV_DOWNLOAD_MAX_ROWS + 12),
        }
    )
    assert_case(
        "truncated",
        truncated_df,
        expected_truncated=True,
        expected_exceeds_size=False,
    )

    huge_text = "x" * (CSV_DOWNLOAD_MAX_BYTES + 4096)
    oversized_df = pd.DataFrame(
        {
            "payload": [huge_text],
            "country": ["DE"],
        }
    )
    assert_case(
        "oversized",
        oversized_df,
        expected_truncated=False,
        expected_exceeds_size=True,
    )

    print("CSV download guardrails: PASS")


if __name__ == "__main__":
    main()
