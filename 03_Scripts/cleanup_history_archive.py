import argparse
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def to_project_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path.resolve())


def collect_candidates(target_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    for path in target_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.name.startswith("~$"):
            continue
        candidates.append(path)
    candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    return candidates


def cleanup_history(
    target_dir: Path,
    keep_latest: int,
    keep_days: int,
    apply: bool,
) -> dict[str, int]:
    if not target_dir.exists():
        raise FileNotFoundError(f"目录不存在: {target_dir}")

    now = time.time()
    retention_seconds = keep_days * 24 * 60 * 60
    candidates = collect_candidates(target_dir)

    keep_set: set[Path] = set(candidates[:keep_latest])
    delete_list: list[Path] = []

    for path in candidates:
        if path in keep_set:
            continue
        age_seconds = now - path.stat().st_mtime
        if age_seconds > retention_seconds:
            delete_list.append(path)

    for path in delete_list:
        if apply:
            path.unlink(missing_ok=True)

    return {
        "totalFiles": len(candidates),
        "keptFiles": len(candidates) - len(delete_list),
        "deletedFiles": len(delete_list),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="清理历史归档目录中的过旧文件（默认 dry-run）。"
    )
    parser.add_argument(
        "--target-dir",
        type=str,
        default="01_RAW_DATA/historyDataArchive",
        help="归档目录路径。",
    )
    parser.add_argument(
        "--keep-latest",
        type=int,
        default=20,
        help="无条件保留的最新文件数量。",
    )
    parser.add_argument(
        "--keep-days",
        type=int,
        default=180,
        help="保留天数阈值（超出且不在最新保留集合将清理）。",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="执行删除；默认仅预览。",
    )
    args = parser.parse_args()

    target_dir = resolve_path(args.target_dir)
    result = cleanup_history(
        target_dir=target_dir,
        keep_latest=max(0, args.keep_latest),
        keep_days=max(1, args.keep_days),
        apply=args.apply,
    )

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"✅ 历史归档清理完成（{mode}）")
    print(f"📁 目录: {to_project_relative(target_dir)}")
    print(f"📦 总文件: {result['totalFiles']}")
    print(f"🧷 保留: {result['keptFiles']}")
    print(f"🗑️ 删除: {result['deletedFiles']}")


if __name__ == "__main__":
    main()
