"""每月 01_RAW_DATA 更新工具。

用法（最常见）：
    python 03_Scripts/data_pipeline/prepare_monthly_raw_update.py \\
        --month 2026-03 \\
        --patch 01_RAW_DATA/新文件.xlsx

baseline 不传则优先找 01_RAW_DATA/baseline/ 下最新的一份；
若 active baseline 暂缺，则回退到 01_RAW_DATA/historyDataArchive/baseline/ 下最新的一份。
"""
import argparse
import re
import shlex
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "01_RAW_DATA"
PROC = ROOT / "04_Processed_data"
ARCHIVE = RAW / "historyDataArchive"


# ── 工具函数 ─────────────────────────────────────────────

def _resolve(text: str) -> Path:
    p = Path(text)
    return p if p.is_absolute() else ROOT / p


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def _normalize_month(text: str) -> str:
    m = re.fullmatch(r"(20\d{2})[-./]?(0?[1-9]|1[0-2])", text.strip())
    if not m:
        raise SystemExit("月份格式无效，需要 YYYY-MM，例如 2026-02")
    return f"{m.group(1)}-{int(m.group(2)):02d}"


def _jato_month(month: str) -> str:
    y, m = month.split("-")
    return f"{y}.{int(m)}"


def _infer_month(path: Path) -> str | None:
    results: list[str] = []
    # dotted / dashed: "2026.1", "2026-01", "2026_02"
    for y, m in re.findall(
        r"(?<!\d)(20\d{2})[._-](0?[1-9]|1[0-2])(?!\d)", path.stem
    ):
        results.append(f"{y}-{int(m):02d}")
    # compact: "202601"
    for y, m in re.findall(
        r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(?!\d)", path.stem
    ):
        results.append(f"{y}-{int(m):02d}")
    if not results:
        return None
    return max(set(results))


def _infer_countries(name: str) -> int | None:
    m = re.search(r"(\d+)\s*\+\s*(\d+)\s*国", name)
    if m:
        return int(m.group(1)) + int(m.group(2))
    m = re.search(r"(\d+)\s*国", name)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*countries", name, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def _find_latest_baseline() -> Path | None:
    for directory in (RAW / "baseline", ARCHIVE / "baseline"):
        if not directory.exists():
            continue
        candidates = [
            p
            for p in directory.glob("*")
            if p.is_file()
            and p.suffix.lower() in {".xlsx", ".xlsm", ".xls"}
            and not p.name.startswith("~$")
        ]
        if not candidates:
            continue
        return max(
            candidates,
            key=lambda p: (_infer_month(p) or "0000-00", p.stat().st_mtime),
        )
    return None


def _stage(src: Path, tgt: Path, dry: bool) -> str:
    if src.resolve() == tgt.resolve(strict=False):
        return "已就位"
    if not dry:
        tgt.parent.mkdir(parents=True, exist_ok=True)
        if tgt.exists():
            tgt.unlink()
        shutil.copy2(src, tgt)
        return "已复制"
    return "将复制"


# ── 主流程 ───────────────────────────────────────────────

def main() -> None:
    pa = argparse.ArgumentParser(
        description="每月 01_RAW_DATA 更新：整理文件 + 生成后续命令",
    )
    pa.add_argument("--month", required=True, help="目标月份 YYYY-MM")
    pa.add_argument("--patch", required=True, help="新的 patch xlsx")
    pa.add_argument(
        "--baseline", default=None,
        help="baseline xlsx（不传则优先用 baseline/，否则回退到 historyDataArchive/baseline/ 下最新的）",
    )
    pa.add_argument(
        "--dry-run", action="store_true", help="只看计划，不动文件",
    )
    args = pa.parse_args()

    month = _normalize_month(args.month)
    jm = _jato_month(month)

    # ---- patch ----
    patch_src = _resolve(args.patch)
    if not patch_src.exists():
        raise SystemExit(f"patch 文件不存在: {patch_src}")

    # ---- baseline ----
    if args.baseline:
        baseline_src = _resolve(args.baseline)
    else:
        baseline_src = _find_latest_baseline()
    if baseline_src is None or not baseline_src.exists():
        raise SystemExit(
            "未找到 baseline。用 --baseline 指定，"
            "或先把 baseline 放到 01_RAW_DATA/baseline/；"
            "如果 active baseline 被清理了，也可先从 01_RAW_DATA/historyDataArchive/baseline/ 恢复。"
        )

    # ---- 推断月份 & 国家数 ----
    bl_month = _infer_month(baseline_src) or "unknown"
    bl_jm = _jato_month(bl_month) if bl_month != "unknown" else "baseline"
    bl_c = _infer_countries(baseline_src.name)
    pa_c = _infer_countries(patch_src.name)

    bl_tag = f"full-{bl_c}countries" if bl_c else "full"
    pa_tag = f"partial-{pa_c}countries" if pa_c else "partial"

    # ---- 目标路径 ----
    bl_tgt = RAW / "baseline" / f"JATO-{bl_jm}-{bl_tag}-baseline.xlsx"
    pa_tgt = RAW / "patches" / month / f"JATO-{jm}-{pa_tag}.xlsx"

    compare_id = f"{bl_month}_vs_{month}"
    review_dir = PROC / "reviews" / "raw_compare" / compare_id
    staging_dir = PROC / "staging" / f"{month}-mixed"

    # ---- 建目录 ----
    for d in [bl_tgt.parent, pa_tgt.parent, review_dir, staging_dir]:
        if not args.dry_run:
            d.mkdir(parents=True, exist_ok=True)

    # ---- 放文件 ----
    bl_action = _stage(baseline_src, bl_tgt, args.dry_run)
    pa_action = _stage(patch_src, pa_tgt, args.dry_run)

    # ---- 生成命令 ----
    compare_cmd = shlex.join([
        "python", "03_Scripts/raw_compare_review.py",
        "--old", _rel(bl_tgt),
        "--new", _rel(pa_tgt),
        "--allow-missing-countries",
        "--output-dir", _rel(review_dir),
    ])
    refresh_cmd = shlex.join([
        "python", "03_Scripts/data_pipeline/run_data_refresh_job.py",
        "--baseline-input", _rel(bl_tgt),
        "--patch-input-files", _rel(pa_tgt),
        "--output", _rel(staging_dir / "jato_full_archive.parquet"),
        "--manifest", _rel(staging_dir / "manifest.json"),
        "--partition-output", _rel(staging_dir / "partitioned_dataset_v1"),
        "--report", _rel(staging_dir / "refresh_job_report.json"),
        "--fingerprint", _rel(staging_dir / "dataset_fingerprint.json"),
        "--incremental", "--skip-benchmark",
    ])

    # ---- 写 plan.md ----
    plan_md = (
        f"# {month} 月度更新计划\n\n"
        f"- 对比: {compare_id}\n"
        f"- baseline: {_rel(bl_tgt)}\n"
        f"- patch: {_rel(pa_tgt)}\n\n"
        f"## 步骤 1 · Raw Compare\n\n```bash\n{compare_cmd}\n```\n\n"
        f"## 步骤 2 · Candidate Refresh\n\n```bash\n{refresh_cmd}\n```\n\n"
        "## 步骤 3 · Promotion\n\n"
        "1. review raw compare 结果\n"
        "2. 确认 staging 无异常后复制到 releases/ 和 canonical 根目录\n"
        "3. 归档到 historyDataArchive/\n"
    )
    plan_path = RAW / "patches" / month / "monthly_update_plan.md"
    if not args.dry_run:
        plan_path.write_text(plan_md, encoding="utf-8")

    # ---- 输出 ----
    tag = "[dry-run] " if args.dry_run else ""
    print(f"{tag}月份: {month}")
    print(f"{tag}baseline: {bl_action} → {_rel(bl_tgt)}")
    print(f"{tag}patch:    {pa_action} → {_rel(pa_tgt)}")
    print()
    print("步骤 1 · Raw Compare:")
    print(compare_cmd)
    print()
    print("步骤 2 · Candidate Refresh:")
    print(refresh_cmd)
    if not args.dry_run:
        print(f"\n计划已写入: {_rel(plan_path)}")


if __name__ == "__main__":
    main()
