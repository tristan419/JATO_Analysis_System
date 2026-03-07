import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_INCLUDE_DIRS = [
    PROJECT_ROOT / "03_Scripts",
    PROJECT_ROOT / "05_DashBoard",
]

IGNORE_PARTS = {
    ".venv",
    "__pycache__",
    ".git",
}


def iter_python_files() -> list[Path]:
    files: list[Path] = []
    for base_dir in DEFAULT_INCLUDE_DIRS:
        if not base_dir.exists():
            continue
        for path in base_dir.rglob("*.py"):
            if any(part in IGNORE_PARTS for part in path.parts):
                continue
            files.append(path)
    return sorted(set(files))


def check_file(path: Path, max_line_length: int) -> list[str]:
    issues: list[str] = []
    content = path.read_text(encoding="utf-8")
    lines = content.splitlines()

    for line_no, line in enumerate(lines, start=1):
        if "\t" in line:
            issues.append(f"{path}:{line_no} 包含 Tab 字符")
        if line.rstrip() != line:
            issues.append(f"{path}:{line_no} 存在行尾空白")
        if len(line) > max_line_length:
            issues.append(
                f"{path}:{line_no} 行长 {len(line)} > {max_line_length}"
            )

    if content and not content.endswith("\n"):
        issues.append(f"{path}: 文件末尾缺少换行")

    return issues


def main() -> None:
    parser = argparse.ArgumentParser(description="轻量代码风格检查。")
    parser.add_argument(
        "--max-line-length",
        type=int,
        default=120,
        help="最大行长度（默认 120）。",
    )
    args = parser.parse_args()

    issues: list[str] = []
    for file_path in iter_python_files():
        issues.extend(check_file(file_path, args.max_line_length))

    if issues:
        print("❌ 代码风格检查失败：")
        for issue in issues:
            print(f"- {issue}")
        raise SystemExit(1)

    print("✅ 代码风格检查通过")


if __name__ == "__main__":
    main()
