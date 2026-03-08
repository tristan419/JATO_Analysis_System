# pyright: reportMissingImports=false

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_ROOT = PROJECT_ROOT / "05_DashBoard"
if str(DASHBOARD_ROOT) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_ROOT))

from dashboard.views import get_default_render_strategy  # noqa: E402


def assert_case(
    name: str,
    large_data_mode: bool,
    row_count: int,
    expected_overview: bool,
    expected_advanced: bool,
) -> None:
    overview_lazy, advanced_lazy = get_default_render_strategy(
        large_data_mode=large_data_mode,
        row_count=row_count,
    )

    print(
        f"{name}: large={large_data_mode} rows={row_count:,} "
        f"overview_lazy={overview_lazy} advanced_lazy={advanced_lazy}"
    )

    if overview_lazy != expected_overview:
        raise AssertionError(
            f"{name} overview mismatch: "
            f"expected={expected_overview} actual={overview_lazy}"
        )

    if advanced_lazy != expected_advanced:
        raise AssertionError(
            f"{name} advanced mismatch: "
            f"expected={expected_advanced} actual={advanced_lazy}"
        )


def main() -> None:
    print("Render strategy defaults regression")

    assert_case(
        name="small-full",
        large_data_mode=False,
        row_count=20_000,
        expected_overview=False,
        expected_advanced=False,
    )
    assert_case(
        name="small-large-mode",
        large_data_mode=True,
        row_count=30_000,
        expected_overview=False,
        expected_advanced=False,
    )
    assert_case(
        name="large-mode-threshold",
        large_data_mode=True,
        row_count=80_000,
        expected_overview=True,
        expected_advanced=True,
    )
    assert_case(
        name="huge-full-mode",
        large_data_mode=False,
        row_count=220_000,
        expected_overview=False,
        expected_advanced=True,
    )

    print("Render strategy defaults: PASS")


if __name__ == "__main__":
    main()
