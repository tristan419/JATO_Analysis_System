import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYTHON_BIN = sys.executable
PARTITIONED_DATASET_DIR = (
    PROJECT_ROOT / "04_Processed_data/partitioned_dataset_v1"
)
FULL_PARQUET_FILE = (
    PROJECT_ROOT / "04_Processed_data/jato_full_archive.parquet"
)


def run(command: list[str], cwd: Path | None = None) -> None:
    joined = " ".join(command)
    print(f"▶ {joined}")
    result = subprocess.run(
        command,
        cwd=str(cwd or PROJECT_ROOT),
        check=False,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def collect_python_files() -> list[str]:
    include_dirs = [
        PROJECT_ROOT / "03_Scripts",
        PROJECT_ROOT / "05_DashBoard",
    ]
    files: list[str] = []
    for base_dir in include_dirs:
        if not base_dir.exists():
            continue
        for path in base_dir.rglob("*.py"):
            if ".venv" in path.parts or "__pycache__" in path.parts:
                continue
            files.append(str(path.relative_to(PROJECT_ROOT)))
    return sorted(set(files))


def has_dashboard_dataset() -> bool:
    has_partitioned = (
        PARTITIONED_DATASET_DIR.exists()
        and any(PARTITIONED_DATASET_DIR.rglob("*.parquet"))
    )
    has_full_parquet = FULL_PARQUET_FILE.exists()
    return bool(has_partitioned or has_full_parquet)


def run_regression_checks() -> None:
    run([PYTHON_BIN, "03_Scripts/regression_csv_download_guardrails.py"])
    run([PYTHON_BIN, "03_Scripts/regression_render_strategy_defaults.py"])
    run([PYTHON_BIN, "03_Scripts/regression_time_selector_consistency.py"])

    if has_dashboard_dataset():
        run([PYTHON_BIN, "03_Scripts/regression_filter_option_pushdown.py"])
    else:
        print(
            "⚠ 跳过 regression_filter_option_pushdown.py: "
            "未发现 dashboard 数据集。"
        )


def main() -> None:
    files = collect_python_files()
    if not files:
        raise SystemExit("未找到可检查的 Python 文件。")

    run([PYTHON_BIN, "03_Scripts/style_check.py"])
    run([PYTHON_BIN, "-m", "py_compile", *files])

    run([PYTHON_BIN, "03_Scripts/elt_worker.py", "--help"])
    run([PYTHON_BIN, "03_Scripts/build_partitioned_dataset.py", "--help"])
    run([PYTHON_BIN, "03_Scripts/run_data_refresh_job.py", "--help"])
    run_regression_checks()

    print("✅ CI smoke checks passed")


if __name__ == "__main__":
    main()
