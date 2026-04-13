from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOOLKIT_ROOT = PROJECT_ROOT / "07_ScrapingToolkit"
DEFAULT_REPORT_ROOT = (
    PROJECT_ROOT / "04_Processed_data" / "msrp_candidate_scope"
)
DEFAULT_DRAFT_ROOT = PROJECT_ROOT / "07_ScrapingToolkit" / "source_drafts"
LEGACY_TOP20_DIR = DEFAULT_DRAFT_ROOT / "top20_batch1"
LEGACY_SPLIT_BRAND_DIRS = (
    DEFAULT_DRAFT_ROOT / "all_market_country_brand_priority_top30",
    DEFAULT_DRAFT_ROOT / "suv_only_country_brand_priority_top30",
)


def _build_variants(
    report_root: Path,
    draft_root: Path,
) -> list[dict[str, Any]]:
    return [
        {
            "name": "all_market",
            "vehicle_category": None,
            "report_dir": report_root / "all_market",
            "draft_dir": draft_root / "all_market_country_model_top30",
        },
        {
            "name": "suv_only",
            "vehicle_category": "SUV",
            "report_dir": report_root / "suv_only",
            "draft_dir": draft_root / "suv_only_country_model_top30",
        },
    ]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Regenerate both all-market and SUV-only MSRP candidate "
            "and backlog outputs."
        ),
    )
    parser.add_argument("--dataset-path", default=None)
    parser.add_argument(
        "--sources-dir",
        default=str(PROJECT_ROOT / "07_ScrapingToolkit" / "sources"),
    )
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--report-root", default=str(DEFAULT_REPORT_ROOT))
    parser.add_argument("--draft-root", default=str(DEFAULT_DRAFT_ROOT))
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=0)
    parser.add_argument("--rollout-batch-size", type=int, default=10)
    parser.add_argument(
        "--keep-legacy-top20",
        action="store_true",
        help=(
            "Do not delete the deprecated source_drafts/top20_batch1 "
            "directory."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    if str(TOOLKIT_ROOT) not in sys.path:
        sys.path.insert(0, str(TOOLKIT_ROOT))

    from jato_scraper.candidate_scope import (
        generate_candidate_scope_report,
        write_candidate_scope_report,
    )
    from jato_scraper.source_bootstrap import generate_source_draft_batch

    parser = build_arg_parser()
    args = parser.parse_args(argv)
    report_root = Path(args.report_root).expanduser().resolve()
    draft_root = Path(args.draft_root).expanduser().resolve()

    generated_variants: list[dict[str, Any]] = []
    for variant in _build_variants(report_root, draft_root):
        report = generate_candidate_scope_report(
            dataset_path=args.dataset_path,
            sources_dir=args.sources_dir,
            top_n=args.top_n,
            project_root=args.project_root,
            vehicle_category=variant["vehicle_category"],
        )
        report_paths = write_candidate_scope_report(
            report,
            output_dir=variant["report_dir"],
        )
        draft_result = generate_source_draft_batch(
            report_path=report_paths["json"],
            output_dir=variant["draft_dir"],
            batch_size=args.batch_size,
            rollout_batch_size=args.rollout_batch_size,
        )
        generated_variants.append(
            {
                "name": variant["name"],
                "vehicle_category": variant["vehicle_category"],
                "report_json": str(report_paths["json"]),
                "report_markdown": str(report_paths["markdown"]),
                "draft_directory": str(
                    draft_result["output_paths"]["directory"]
                ),
                "draft_summary_json": str(
                    draft_result["output_paths"]["summary_json"]
                ),
                "draft_summary_markdown": str(
                    draft_result["output_paths"]["summary_markdown"]
                ),
                "candidate_count": report["candidate_count"],
                "country_count": report["country_count"],
                "draft_count": len(draft_result["opportunities"]),
            }
        )

    if not args.keep_legacy_top20 and LEGACY_TOP20_DIR.exists():
        shutil.rmtree(LEGACY_TOP20_DIR)
    for legacy_dir in LEGACY_SPLIT_BRAND_DIRS:
        if legacy_dir.exists():
            shutil.rmtree(legacy_dir)

    print(
        json.dumps(
            {"variants": generated_variants},
            ensure_ascii=False,
            indent=2,
        )
    )
    if not args.keep_legacy_top20:
        print(f"REMOVED_LEGACY_TOP20={LEGACY_TOP20_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
