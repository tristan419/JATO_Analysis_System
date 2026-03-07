import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYTHON_BIN = sys.executable


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


def main() -> None:
    files = collect_python_files()
    if not files:
        raise SystemExit("未找到可检查的 Python 文件。")

    run([PYTHON_BIN, "03_Scripts/style_check.py"])
    run([PYTHON_BIN, "-m", "py_compile", *files])

    run([PYTHON_BIN, "03_Scripts/elt_worker.py", "--help"])
    run([PYTHON_BIN, "03_Scripts/build_partitioned_dataset.py", "--help"])
    run([PYTHON_BIN, "03_Scripts/run_data_refresh_job.py", "--help"])

    print("✅ CI smoke checks passed")


if __name__ == "__main__":
    main()
